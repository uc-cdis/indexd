import re
import jsonschema

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from indexd.auth import authorize_decorator
from indexd.errors import UserError

from .schema import PUT_RECORD_SCHEMA

router = APIRouter(tags=["alias"])


def set_alias_config(app):
    alias_config = app.settings["config"]["ALIAS"]
    router.alias_driver = alias_config["driver"]


ACCEPTABLE_HASHES = {
    "md5": re.compile(r"^[0-9a-f]{32}$").match,
    "sha1": re.compile(r"^[0-9a-f]{40}$").match,
    "sha256": re.compile(r"^[0-9a-f]{64}$").match,
    "sha512": re.compile(r"^[0-9a-f]{128}$").match,
}


def validate_hashes(**hashes):
    if not all(h in ACCEPTABLE_HASHES for h in hashes):
        raise UserError("invalid hash types specified")
    if not all(ACCEPTABLE_HASHES[h](v) for h, v in hashes.items()):
        raise UserError("invalid hash values specified")


@router.get("/alias/")
async def get_alias(request: Request):
    """
    Returns a list of records.
    """
    limit = request.query_params.get("limit")
    try:
        limit = 100 if limit is None else int(limit)
    except ValueError:
        raise UserError("Limit must be an integer.")

    if limit <= 0 or limit > 1024:
        raise UserError("Limit must be between 1 and 1024.")

    size = request.query_params.get("size")
    try:
        size = size if size is None else int(size)
    except ValueError:
        raise UserError("Size must be an integer.")

    if size is not None and size < 0:
        raise UserError("Size must be >= 0.")

    start = request.query_params.get("start")

    hashes_raw = request.query_params.getlist("hash")
    hashes = {h: v for h, v in (x.split(":", 1) for x in hashes_raw)}

    validate_hashes(**hashes)
    hashes = hashes if hashes else None

    aliases = router.alias_driver.aliases(
        start=start, limit=limit, size=size, hashes=hashes
    )

    base = {
        "aliases": aliases,
        "limit": limit,
        "start": start,
        "size": size,
        "hashes": hashes,
    }

    return JSONResponse(content=base, status_code=200)


@router.put("/alias/{record:path}", dependencies=[Depends(authorize_decorator)])
async def put_alias_record(record: str, request: Request):
    """
    Create or replace an existing record.
    """

    body = await request.json()
    try:
        jsonschema.validate(body, PUT_RECORD_SCHEMA)
    except jsonschema.ValidationError as err:
        raise UserError(err)

    rev = request.query_params.get("rev")

    size = body.get("size")
    hashes = body.get("hashes")
    release = body.get("release")
    metastring = body.get("metadata")
    host_authorities = body.get("host_authorities")
    keeper_authority = body.get("keeper_authority")

    record_res, rev_res = router.alias_driver.upsert(
        record,
        rev,
        size=size,
        hashes=hashes,
        release=release,
        metastring=metastring,
        host_authorities=host_authorities,
        keeper_authority=keeper_authority,
    )

    ret = {"name": record_res, "rev": rev_res}

    return JSONResponse(content=ret, status_code=200)


@router.delete("/alias/{record:path}", dependencies=[Depends(authorize_decorator)])
async def delete_alias_record(record: str, request: Request):
    """
    Delete an alias.
    """
    rev = request.query_params.get("rev")
    router.alias_driver.delete(record, rev)
    return JSONResponse(content="", status_code=200)

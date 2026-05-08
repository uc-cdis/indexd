from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse

from indexd.blueprint import dist_get_record
from indexd.errors import UserError
from indexd.alias.errors import NoRecordFound as AliasNoRecordFound
from indexd.index.errors import NoRecordFound as IndexNoRecordFound

router = APIRouter(prefix="/ga4gh/dos/v1", tags=["dos"])


def set_dos_config(app):
    index_config = app.settings["INDEX"]
    alias_config = app.settings["ALIAS"]
    router.index_driver = index_config["driver"]
    router.alias_driver = alias_config["driver"]
    router.dist = app.settings.get("DIST", [])


def indexd_to_dos(record):
    data_object = {"id": record["did"], "description": "", "mime_type": ""}

    if "file_name" in record:
        data_object["name"] = record["file_name"]
    if "created_date" in record:
        data_object["created"] = record["created_date"]
    if "updated_date" in record:
        data_object["updated"] = record["updated_date"]
    if "rev" in record:
        data_object["version"] = record["rev"]
    if "size" in record:
        data_object["size"] = record["size"]
    if "alias" in record:
        data_object["aliases"] = record["alias"]

    # parse out checksums
    data_object["checksums"] = []
    for k in record.get("hashes", {}):
        data_object["checksums"].append({"checksum": record["hashes"][k], "type": k})

    # parse out urls
    data_object["urls"] = []
    for url in record.get("urls", []):
        url_object = {"url": url}
        if "metadata" in record and record["metadata"]:
            url_object["system_metadata"] = record["metadata"]
        if (
            "urls_metadata" in record
            and url in record["urls_metadata"]
            and record["urls_metadata"][url]
        ):
            url_object["user_metadata"] = record["urls_metadata"][url]
        data_object["urls"].append(url_object)

    return {"data_object": data_object}


@router.get("/dataobjects/{record:path}")
async def get_dos_record(record: str):
    """
    Returns a record from the local ids, alias, or global resolvers.
    Returns DOS Schema
    """

    try:
        ret = router.index_driver.get(record)
        # record may be a baseID or a DID / GUID. If record is a baseID, ret["did"] is the latest GUID for that record.
        ret["alias"] = router.index_driver.get_aliases_for_did(ret["did"])
    except IndexNoRecordFound:
        try:
            ret = router.index_driver.get_by_alias(record)
            ret["alias"] = router.index_driver.get_aliases_for_did(ret["did"])
        except IndexNoRecordFound:
            try:
                ret = router.alias_driver.get(record)
            except AliasNoRecordFound:
                if not router.dist:
                    raise HTTPException(status_code=404, detail="No record found")
                ret = dist_get_record(record)

    dos_ret = indexd_to_dos(ret)
    return JSONResponse(content=dos_ret, status_code=200)


@router.get("/dataobjects")
async def list_dos_records(request: Request):
    """
    Returns a record from the local ids, alias, or global resolvers.
    Returns DOS Schema
    """

    start = request.query_params.get("page_token")
    limit = request.query_params.get("page_size")

    try:
        limit = 100 if limit is None else int(limit)
    except ValueError:
        raise UserError("Limit must be an integer.")

    if limit <= 0 or limit > 1024:
        raise UserError("Limit must be between 1 and 1024.")

    url = request.query_params.get("url")
    hashes = None

    checksum = request.query_params.get("checksum")
    if checksum:
        try:
            type_, checksum_value = checksum.split(":", 1)
            hashes = {type_: checksum_value}
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid checksum format")

    records = router.index_driver.ids(start=start, limit=limit, urls=url, hashes=hashes)

    for record in records:
        record["alias"] = router.index_driver.get_aliases_for_did(record["did"])

    ret = {"data_objects": [indexd_to_dos(record)["data_object"] for record in records]}

    return JSONResponse(content=ret, status_code=200)

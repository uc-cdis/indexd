from fastapi import APIRouter, Request, HTTPException, status
from fastapi.responses import JSONResponse

from indexclient.client import IndexClient
from doiclient.client import DOIClient
from dosclient.client import DOSClient
from hsclient.client import HSClient

from indexd.utils import hint_match
from indexd.alias.errors import NoRecordFound as AliasNoRecordFound
from indexd.index.errors import NoRecordFound as IndexNoRecordFound

router = APIRouter(tags=["cross"])

router.index_driver = None
router.alias_driver = None
router.dist = []


def set_cross_config(app):
    index_config = app.settings["INDEX"]
    alias_config = app.settings["ALIAS"]
    router.index_driver = index_config["driver"]
    router.alias_driver = alias_config["driver"]
    router.dist = app.settings.get("DIST", [])


@router.get("/alias/{alias:path}")
async def get_alias(alias: str):
    """
    Return alias associated information.
    """
    info = router.alias_driver.get(alias)

    start = 0
    limit = 100

    size = info["size"]
    hashes = info["hashes"]

    urls = router.index_driver.get_urls(
        size=size, hashes=hashes, start=start, limit=limit
    )

    info.update({"urls": urls, "start": start, "limit": limit})

    return JSONResponse(content=info, status_code=200)


@router.get("/{record:path}")
async def get_record(record: str, request: Request):
    """
    Returns a record from the local ids, alias, or global resolvers.
    """

    ret = None

    try:
        ret = router.index_driver.get_with_nonstrict_prefix(record)
    except IndexNoRecordFound:
        try:
            ret = router.index_driver.get_by_alias(record)
        except IndexNoRecordFound:
            try:
                ret = router.alias_driver.get(record)
            except AliasNoRecordFound:
                if not router.dist or "no_dist" in request.query_params:
                    raise HTTPException(
                        status_code=404, detail="No record found"
                    )  # check_later
                ret = dist_get_record(record, request)

    return JSONResponse(content=ret, status_code=200)


def dist_get_record(record):
    # Sort the list of distributed ID services
    # Ones with which the request matches a hint will be first
    # Followed by those that don't match the hint

    sorted_dist = sorted(
        router.dist, key=lambda k: hint_match(record, k["hints"]), reverse=True
    )

    for indexd in sorted_dist:
        try:
            if indexd["type"] == "doi":
                # Digital Object Identifier
                fetcher_client = DOIClient(baseurl=indexd["host"])
                res = fetcher_client.get(record)
            elif indexd["type"] == "dos":
                # Data Object Service
                fetcher_client = DOSClient(baseurl=indexd["host"])
                res = fetcher_client.get(record)
            elif indexd["type"] == "hs":
                # HydroShare and CommonsShare
                fetcher_client = HSClient(baseurl=indexd["host"])
                res = fetcher_client.get(record)
            else:
                fetcher_client = IndexClient(baseurl=indexd["host"])
                res = fetcher_client.global_get(record, no_dist=True)
        except Exception:
            continue

        if res:
            json = res.to_json()
            json["from_index_service"] = {
                "host": indexd["host"],
                "name": indexd["name"],
            }
            return json

    raise IndexNoRecordFound("No record found")

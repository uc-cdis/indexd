import uuid
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse

router = APIRouter(tags=["guid"])

router.index_driver = None


def set_guid_config(app):
    router.index_driver = app.settings["INDEX"]["driver"]


@router.get("/guid/mint")
async def mint_guid(request: Request):
    """
    Mint a GUID that is valid for this instance of indexd.
    This endpoint is to allow generating valid GUIDs to be indexed WITHOUT actually creating a new record yet.
    Allows for a `count` query parameter to get bulk GUIDs up to some limit.
    """
    count_param = request.query_params.get("count", "1")
    max_count = 10000
    try:
        count = int(count_param)
    except Exception:
        raise HTTPException(
            status_code=400, detail=f"Count {count_param} is not a valid integer"
        )

    if count < 0:
        raise HTTPException(
            status_code=400, detail="You cannot provide a count less than 0"
        )
    elif count > max_count:
        raise HTTPException(
            status_code=400,
            detail=f"You cannot provide a count greater than {max_count}",
        )

    guids = [_get_prefix() + str(uuid.uuid4()) for _ in range(count)]
    return JSONResponse(content={"guids": guids}, status_code=200)


@router.get("/guid/prefix")
async def get_prefix():
    """
    Get the prefix for this instance of indexd.
    """
    return JSONResponse(content={"prefix": _get_prefix()}, status_code=200)


def _get_prefix():
    """
    Return prefix if it's configured to be prepended to all GUIDs and NOT
    set as an alias
    """
    prefix = ""
    driver_config = router.index_driver.config
    if driver_config.get("PREPEND_PREFIX") and not driver_config.get(
        "ADD_PREFIX_ALIAS"
    ):
        prefix = driver_config.get("DEFAULT_PREFIX", "")
    return prefix

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from indexd.errors import UserError
from indexd.index.drivers.query.urls import AlchemyURLsQueryDriver
from indexd.index.drivers.single_table_alchemy import SingleTableSQLAlchemyIndexDriver

router = APIRouter(tags=["urls"])

router.logger = None
router.driver = None


def set_urls_config(app):
    driver = app.settings["INDEX"]["driver"]
    router.logger = getattr(app, "logger", None)
    router.driver = (
        driver
        if type(driver) == SingleTableSQLAlchemyIndexDriver
        else AlchemyURLsQueryDriver(driver)
    )


@router.get("/q")
async def query(request: Request):
    """Queries indexes based on URLs
    Params:
        exclude (str): only include documents (did) with urls that does not match this pattern
        include (str): only include documents (did) with a url matching this pattern
        version (str): return only records with version number
        fields (str): comma separated list of fields to return, if not specified return all fields
        limit (str): max results to return
        offset (str): where to start the next query from
    Returns:
        Response: json list of matching entries
            `
                [
                    {"did": "AAAA-BB", "rev": "1ADs" "urls": ["s3://some-randomly-awesome-url"]},
                    {"did": "AAAA-CC", "rev": "2Nsf", "urls": ["s3://another-randomly-awesome-url"]}
                ]
            `
    """
    args = dict(request.query_params)
    try:
        record_list = router.driver.query_urls(**args)
    except Exception as err:
        raise UserError(str(err))
    return JSONResponse(content=record_list, status_code=200)


@router.get("/metadata/q")
async def query_metadata(request: Request):
    """Queries indexes by URLs metadata key and value
    Params:
        key (str): metadata key
        value (str): metadata value for key
        url (str): full url or pattern for limit to
        fields (str): comma separated list of fields to return, if not specified return all fields
        version (str): filter only records with a version number
        limit (str): max results to return
        offset (str): where to start the next query from
    Returns:
        Response: json list of matching entries
            `
                [
                    {"did": "AAAA-BB", "rev": "1ADs" "urls": ["s3://some-randomly-awesome-url"]},
                    {"did": "AAAA-CC", "rev": "2Nsf", "urls": ["s3://another-randomly-awesome-url"]}
                ]
            `
    """
    args = dict(request.query_params)
    try:
        record_list = router.driver.query_metadata_by_key(**args)
    except Exception as err:
        raise UserError(str(err))
    return JSONResponse(content=record_list, status_code=200)

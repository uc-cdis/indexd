"""Bulk operations for indexd"""

import json

from typing import List
from fastapi import APIRouter, Depends, Body
from sqlalchemy.orm import joinedload

from indexd.errors import UserError
from indexd.index.drivers.alchemy import IndexRecord, IndexRecordUrl

router = APIRouter(prefix="/bulk", tags=["bulk"])


def set_bulk_config(app):
    if "INDEX" in app.settings["config"]:
        router.index_driver = app.settings["config"]["INDEX"]["driver"]


def get_index_driver():
    return router.index_driver


@router.post("/documents", response_model=List[dict])
async def bulk_get_documents(
    ids: List[str] = Body(..., description="List of record DIDs"),
    index_driver=Depends(get_index_driver),
):
    if not ids:
        raise UserError("No ids provided.")
    if not isinstance(ids, list):
        raise UserError("IDs is not a list.")

    # ensure strings
    guids = [str(guid) for guid in ids]

    docs = blueprint.index_driver.get_bulk(guid_list=guids)

    return flask.Response(json.dumps(docs), 200, mimetype="application/json")

    return docs

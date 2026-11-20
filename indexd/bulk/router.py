"""Bulk operations for indexd"""

import json

from typing import List
from fastapi import APIRouter, Depends, Body

from indexd.errors import UserError

router = APIRouter(prefix="/bulk", tags=["bulk"])


def set_bulk_config(app):
    if "INDEX" in app.settings["config"]:
        router.index_driver = app.settings["config"]["INDEX"]["driver"]


async def get_index_driver():
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

    # Use the async driver's get_bulk method instead of querying the session directly in the route
    docs = await index_driver.get_bulk(ids)

    return docs

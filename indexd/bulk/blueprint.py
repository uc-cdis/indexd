"""Bulk operations for indexd"""

import json

from typing import List
from fastapi import APIRouter, Depends, Body
from sqlalchemy.orm import joinedload

from indexd.errors import UserError
from indexd.index.drivers.alchemy import IndexRecord, IndexRecordUrl

router = APIRouter(prefix="/bulk", tags=["bulk"])


def set_bulk_config(app):
    router.index_driver = app.settings["INDEX"]["driver"]


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

    with index_driver.session as session:
        query = session.query(IndexRecord)
        query = query.options(
            joinedload(IndexRecord.urls).joinedload(IndexRecordUrl.url_metadata)
        )
        query = query.options(joinedload(IndexRecord.acl))
        query = query.options(joinedload(IndexRecord.authz))
        query = query.options(joinedload(IndexRecord.hashes))
        query = query.options(joinedload(IndexRecord.index_metadata))
        query = query.options(joinedload(IndexRecord.aliases))
        query = query.filter(IndexRecord.did.in_(ids))

        docs = [q.to_document_dict() for q in query]

    return docs

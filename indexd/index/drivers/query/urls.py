from sqlalchemy import func, and_

from indexd.errors import UserError
from indexd.index.drivers.alchemy import (
    IndexRecord,
    IndexRecordUrl,
    IndexRecordUrlMetadata, _enforce_rbac, IndexRecordAuthz,
)
from indexd.index.drivers.query import URLsQueryDriver


driver_query_map = {
    "sqlite": dict(array_agg=func.group_concat, string_agg=func.group_concat),
    "postgresql": dict(array_agg=func.array_agg, string_agg=func.string_agg),
}


class AlchemyURLsQueryDriver(URLsQueryDriver):
    """SQLAlchemy based impl"""

    def __init__(self, alchemy_driver):
        """Queries index records based on URL
        Args:
            alchemy_driver (indexd.index.drivers.alchemy.SQLAlchemyIndexDriver):
        """
        self.driver = alchemy_driver

    def query_urls(
        self,
        exclude=None,
        include=None,
        versioned=None,
        offset=0,
        limit=1000,
        fields="did,urls",
        **kwargs
    ):
        if kwargs:
            raise UserError(
                "Unexpected query parameter(s) {}".format(list(kwargs.keys()))
            )

        versioned = (
            versioned.lower() in ["true", "t", "yes", "y"] if versioned else None
        )

        any_authz, authz = _enforce_rbac([])

        with self.driver.session as session:
            # special database specific functions dependent of the selected dialect
            q_func = driver_query_map.get(session.bind.dialect.name)

            query = session.query(
                IndexRecordUrl.did, q_func["string_agg"](IndexRecordUrl.url, ",")
            )

            # Only join IndexRecord once, using outerjoin if versioned filtering is needed
            if versioned is not None or any_authz:
                query = query.outerjoin(IndexRecord, IndexRecord.did == IndexRecordUrl.did)

            # add version filter if versioned is not None
            if versioned is True:  # retrieve only those with a version number
                query = query.filter(IndexRecord.version.isnot(None))
            elif versioned is False:  # retrieve only those without a version number
                query = query.filter(~IndexRecord.version.isnot(None))

            query = query.group_by(IndexRecordUrl.did)

            # add url filters
            if include and exclude:
                query = query.having(
                    and_(
                        ~q_func["string_agg"](IndexRecordUrl.url, ",").contains(
                            exclude
                        ),
                        q_func["string_agg"](IndexRecordUrl.url, ",").contains(include),
                    )
                )
            elif include:
                query = query.having(
                    q_func["string_agg"](IndexRecordUrl.url, ",").contains(include)
                )
            elif exclude:
                query = query.having(
                    ~q_func["string_agg"](IndexRecordUrl.url, ",").contains(exclude)
                )

            # add authz filter
            if any_authz:
                # if any_authz is set, we want to filter records that have ANY of the authz elements
                # was getting the following warning
                #  SAWarning: SELECT statement has a cartesian product between FROM element(s) "index_record" and FROM element "index_record_url".  Apply join condition(s) between each element to resolve. .all()
                # This warning means the query is selecting from multiple tables (index_record and index_record_url) without a proper join condition, causing a cartesian product. To fix this, so we explicitly join IndexRecordUrl to IndexRecord using their relationship.
                # originally it was:
                # Add an explicit join between IndexRecordUrl and IndexRecord
                sub = session.query(IndexRecordAuthz.did).filter(
                    IndexRecordAuthz.resource.in_(any_authz)
                )
                query = query.filter(IndexRecord.did.in_(sub.with_entities(IndexRecordAuthz.did).statement))


            # [('did', 'urls')]

            record_list = (
                query.order_by(IndexRecordUrl.did.asc())
                .offset(offset)
                .limit(limit)
                .all()
            )
            # alternatively, we could have used the following query:
            # record_list = (
            #     query.join(IndexRecord, IndexRecord.did == IndexRecordUrl.did)
            #     .order_by(IndexRecordUrl.did.asc())
            #     .offset(offset)
            #     .limit(limit)
            #     .all()
            # )
        return self._format_response(fields, record_list)

    def query_metadata_by_key(
        self,
        key,
        value,
        url=None,
        versioned=None,
        offset=0,
        limit=1000,
        fields="did,urls,rev",
        **kwargs
    ):
        if kwargs:
            raise UserError(
                "Unexpected query parameter(s) {}".format(list(kwargs.keys()))
            )

        versioned = (
            versioned.lower() in ["true", "t", "yes", "y"] if versioned else None
        )
        any_authz, authz = _enforce_rbac([])

        with self.driver.session as session:
            query = session.query(
                IndexRecordUrlMetadata.did, IndexRecordUrlMetadata.url, IndexRecord.rev
            ).filter(
                IndexRecord.did == IndexRecordUrlMetadata.did,
                IndexRecordUrlMetadata.key == key,
                IndexRecordUrlMetadata.value == value,
            )

            # add authz filter
            if any_authz:
                # if any_authz is set, we want to filter records that have ANY of the authz elements
                sub = session.query(IndexRecordAuthz.did).filter(
                    IndexRecordAuthz.resource.in_(any_authz)
                )
                query = query.filter(IndexRecord.did.in_(sub.subquery().select()))

            # filter by version
            if versioned is True:
                query = query.filter(IndexRecord.version.isnot(None))
            elif versioned is False:
                query = query.filter(~IndexRecord.version.isnot(None))

            # add url filter
            if url:
                query = query.filter(
                    IndexRecordUrlMetadata.url.like("%{}%".format(url))
                )

            # [('did', 'url', 'rev')]
            record_list = (
                query.order_by(IndexRecordUrlMetadata.did.asc())
                .offset(offset)
                .limit(limit)
                .all()
            )
        return self._format_response(fields, record_list)

    @staticmethod
    def _format_response(requested_fields, record_list):
        """loops through the query result and removes undesired columns and converts result of urls string_agg to list
        Args:
            requested_fields (str): comma separated list of fields to return, if not specified return all fields
            record_list (list(tuple): must be of the form [(did, urls, rev)], rev is not required for urls query
        Returns:
            list[dict]: list of response dicts
        """
        result = []
        provided_fields_dict = {k: 1 for k in requested_fields.split(",")}
        for record in record_list:
            resp_dict = {}
            if provided_fields_dict.get("did"):
                resp_dict["did"] = record[0]
            if provided_fields_dict.get("urls"):
                resp_dict["urls"] = record[1].split(",") if record[1] else []

            # check if record is returned in tuple
            if provided_fields_dict.get("rev") and len(record) == 3:
                resp_dict["rev"] = record[2]
            result.append(resp_dict)
        return result

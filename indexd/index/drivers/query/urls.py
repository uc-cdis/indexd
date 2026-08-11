from sqlalchemy import func, and_, select

from indexd.errors import UserError
from indexd.index.drivers.alchemy import (
    IndexRecord,
    IndexRecordUrl,
    IndexRecordUrlMetadata,
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

    async def query_urls(
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

        async with self.driver.session as session:
            # special database specific functions dependent of the selected dialect
            q_func = driver_query_map.get(session.bind.dialect.name)

            string_agg_fn = q_func["string_agg"](IndexRecordUrl.url, ",")

            stmt = select(IndexRecordUrl.did, string_agg_fn)

            # add version filter if versioned is not None
            if versioned is True:  # retrieve only those with a version number
                stmt = stmt.outerjoin(IndexRecord)
                stmt = stmt.filter(IndexRecord.version.isnot(None))
            elif versioned is False:  # retrieve only those without a version number
                stmt = stmt.outerjoin(IndexRecord)
                stmt = stmt.filter(~IndexRecord.version.isnot(None))

            stmt = stmt.group_by(IndexRecordUrl.did)

            # add url filters
            if include and exclude:
                stmt = stmt.having(
                    and_(
                        ~string_agg_fn.contains(exclude),
                        string_agg_fn.contains(include),
                    )
                )
            elif include:
                stmt = stmt.having(string_agg_fn.contains(include))
            elif exclude:
                stmt = stmt.having(~string_agg_fn.contains(exclude))

            # execute and fetch all rows asynchronously
            stmt = stmt.order_by(IndexRecordUrl.did.asc()).offset(offset).limit(limit)
            result = await session.execute(stmt)
            record_list = result.all()

        return self._format_response(fields, record_list)

    async def query_metadata_by_key(
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

        async with self.driver.session as session:
            stmt = select(
                IndexRecordUrlMetadata.did, IndexRecordUrlMetadata.url, IndexRecord.rev
            ).filter(
                IndexRecord.did == IndexRecordUrlMetadata.did,
                IndexRecordUrlMetadata.key == key,
                IndexRecordUrlMetadata.value == value,
            )

            # filter by version
            if versioned is True:
                stmt = stmt.filter(IndexRecord.version.isnot(None))
            elif versioned is False:
                stmt = stmt.filter(~IndexRecord.version.isnot(None))

            # add url filter
            if url:
                stmt = stmt.filter(IndexRecordUrlMetadata.url.like("%{}%".format(url)))

            # execute and fetch all rows asynchronously
            stmt = (
                stmt.order_by(IndexRecordUrlMetadata.did.asc())
                .offset(offset)
                .limit(limit)
            )
            result = await session.execute(stmt)
            record_list = result.all()

        return self._format_response(fields, record_list)

    @staticmethod
    def _format_response(requested_fields, record_list):
        """loops through the query result and removes undesired columns and converts result of urls string_agg to list
        Args:
            requested_fields (str): comma separated list of fields to return, if not specified return all fields
            record_list (list(tuple]): must be of the form [(did, urls, rev)], rev is not required for urls query
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

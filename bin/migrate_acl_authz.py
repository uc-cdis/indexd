"""
This script is used to migrate the `acl` field in indexd to the new `authz` field which
will be used in combination with arborist to handle access control on indexd records.

The `authz` field should consist of a list of resource tags (as defined by
arborist---see arborist's readme at https://github.com/uc-cdis/arborist for more info),
with the meaning that a user trying to access the data file pointed to by this record
must have access to all the resources listed. These resources may be projects or consent
codes or some other mechanism for specifying authorization.

In terms of the migration, it isn't discernable from indexd itself whether the items
listed in the `acl` are programs or projects. For this reason we need access to the
sheepdog tables with "core data"/"metadata", so we can look up which is which. Then, if
the record previously had both a program and a project, since the authz field requires
access to all the listed items, only the project should end up in `authz` (since
requiring the program would omit access to users who can access only the project).

Furthermore, there are two ways to represent the Arborist resources that go into
`authz`: the path (human-readable string) and the tag (random string, pseudo-UUID). The
tags are what we want to ultimately put into the `authz` field, since these are
persistent whereas the path could change if resources are renamed.
"""

import argparse
import os
import sys

from cdislogging import get_logger
import requests
import sqlalchemy
from sqlalchemy import and_, func
from sqlalchemy.engine import create_engine
from sqlalchemy.exc import OperationalError

from indexd.index.drivers.alchemy import IndexRecord, IndexRecordAuthz

from yaml import safe_load

logger = get_logger("migrate_acl_authz", log_level="debug")


def main():
    args = parse_args()
    sys.path.append(args.path)
    try:
        from local_settings import settings
    except ImportError:
        logger.info("Can't import local_settings, import from default")
        from indexd.default_settings import settings
    driver = settings["config"]["INDEX"]["driver"]
    try:
        acl_converter = ACLConverter(
            args.arborist,
            getattr(args, "sheepdog"),
            getattr(args, "use_tags"),
            getattr(args, "user_yaml_path"),
        )
    except EnvironmentError:
        logger.error("can't continue without database connection")
        sys.exit(1)

    if hasattr(args, "start_did"):
        logger.info("starting at did {}".format(args.start_did))

    with driver.session as session:
        q = session.query(IndexRecord)
        wq = windowed_query(
            session,
            q,
            IndexRecord.did,
            int(args.chunk_size),
            start=getattr(args, "start_did"),
        )
        for record in wq:
            if not record.acl:
                logger.info(
                    "record {} has no acl, setting authz to empty".format(record.did)
                )
                record.authz = []
                continue
            try:
                authz = acl_converter.acl_to_authz(record)
                if authz:
                    record.authz = [IndexRecordAuthz(did=record.did, resource=authz)]
                    logger.info("updated authz for {} to {}".format(record.did, authz))
                else:
                    record.authz = []
                    logger.info("updated authz for {} to empty list".format(record.did))
                session.add(record)
            except EnvironmentError as e:
                msg = "problem adding authz for record {}: {}".format(record.did, e)
                logger.error(msg)
    logger.info("finished migrating")
    return


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--path", default="/var/www/indexd/", help="path to find local_settings.py"
    )
    parser.add_argument(
        "--sheepdog-db", dest="sheepdog", help="URI for the sheepdog database"
    )
    parser.add_argument(
        "--arborist-url", dest="arborist", help="URL for the arborist service"
    )
    parser.add_argument(
        "--tags",
        dest="use_tags",
        help="Whether to use arborist tags. If set to False, the resource paths will be used",
        default=False,
    )
    parser.add_argument(
        "--chunk-size",
        dest="chunk_size",
        type=int,
        default=1000,
        help="number of records to process at once",
    )
    parser.add_argument(
        "--start-did",
        dest="start_did",
        help="did to start at (records processed in lexographical order)",
    )
    parser.add_argument(
        "--user-yaml-path",
        dest="user_yaml_path",
        help="path to user yaml for pulling authz mapping",
    )
    return parser.parse_args()


class ACLConverter(object):
    def __init__(
        self, arborist_url, sheepdog_db=None, use_tags=False, user_yaml_path=None
    ):
        self.arborist_url = arborist_url.rstrip("/")
        self.programs = set()
        self.projects = dict()
        self.namespace = ""
        if os.getenv("AUTH_NAMESPACE"):
            self.namespace = "/" + os.getenv("AUTH_NAMESPACE").lstrip("/")
            logger.info("using namespace {}".format(self.namespace))
        else:
            logger.info("not using any auth namespace")
        self.use_sheepdog_db = bool(sheepdog_db)
        self.mapping = {}

        if user_yaml_path:
            with open(user_yaml_path, "r") as f:
                user_yaml = safe_load(f)
                user_yaml_authz = user_yaml.get("authz", dict())
                if not user_yaml_authz:
                    user_yaml_authz = user_yaml.get("rbac", dict())

                project_to_resource = user_yaml_authz.get(
                    "user_project_to_resource", dict()
                )
                self.mapping = project_to_resource

        logger.info(f"got mapping: {self.mapping}")

        # if "use_tags" is True, map resource paths to tags in arborist so
        # we can save http calls
        self.use_arborist_tags = use_tags
        self.arborist_resources = dict()

        if sheepdog_db:
            engine = create_engine(sheepdog_db, echo=False)
            try:
                connection = engine.connect()
            except OperationalError:
                raise EnvironmentError(
                    "couldn't connect to sheepdog db using the provided URI"
                )
            result = connection.execute(
                "SELECT _props->>'name' as name from node_program;"
            )
            for row in result:
                self.programs.add(row["name"])
            result = connection.execute(
                """
                SELECT
                    project._props->>'name' AS name,
                    program._props->>'name' AS program
                FROM node_project AS project
                JOIN edge_projectmemberofprogram AS edge ON edge.src_id = project.node_id
                JOIN node_program AS program ON edge.dst_id = program.node_id;
            """
            )
            for row in result:
                self.projects[row["name"]] = row["program"]
            connection.close()
            logger.info("found programs: {}".format(list(self.programs)))
            projects_log = [
                "{} (from program {})".format(project, program)
                for project, program in self.projects.items()
            ]
            logger.info("found projects: [{}]".format(", ".join(projects_log)))

    def is_program(self, acl_item):
        return acl_item in self.programs

    def acl_to_authz(self, record):
        path = None
        programs_found = 0
        projects_found = 0
        for acl_object in record.acl:
            acl_item = acl_object.ace
            # we'll try to do some sanitizing here since the record ACLs are sometimes
            # really mis-formatted, like `["u'phs000123'"]`, or have spaces left in
            acl_item = acl_item.strip(" ")
            acl_item = acl_item.lstrip("u'")
            # Pauline 2019-12-10 Disabling this, causes a bug when removing "-"
            # if acl_item != "*":
            #     acl_item = re.sub(r"\W+", "", acl_item)

            # update path based on ACL entry
            if not acl_item:
                # ignore empty string
                continue
            # prefer user.yaml authz mapping (if provided)
            elif acl_item in self.mapping:
                path = self.mapping[acl_item]
                projects_found += 1
                break
            elif acl_item == "*":
                # if there's a * it should just be open. return early
                path = "/open"
                break
            elif not self.use_sheepdog_db or self.is_program(acl_item):
                # if we don't have sheepdog we have to assume everything is a "program".
                if projects_found == 0:
                    # we only want to set the path to a program if we haven't found a
                    # path for a project already.
                    path = "/programs/{}".format(acl_item)
                programs_found += 1
            elif acl_item in self.projects:
                # always want to update to project if possible
                path = "/programs/{}/projects/{}".format(
                    self.projects[acl_item], acl_item
                )
                projects_found += 1
                break

        if not path:
            logger.error(
                "couldn't get `authz` for record {}; setting as empty".format(
                    record.did
                )
            )
            return None

        if programs_found > 1:
            logger.error("found multiple programs in ACL for {}".format(record.did))
        if projects_found > 1:
            logger.error("found multiple projects in ACL for {}".format(record.did))

        if self.namespace:
            path = self.namespace + path

        if path not in self.arborist_resources:
            # add `?p` to create parent resources as necessary
            url = "{}/resource/?p".format(self.arborist_url)
            try:
                resource = {"path": path}
                response = requests.post(url, timeout=5, json=resource)
            except requests.exceptions.Timeout:
                logger.error(
                    "couldn't hit arborist to look up resource (timed out): {}".format(
                        url
                    )
                )
                raise EnvironmentError("couldn't reach arborist; request timed out")
            tag = None
            try:
                logger.debug(
                    "got {} from arborist: {}".format(
                        response.status_code, response.json()
                    )
                )
                if response.status_code == 409:
                    # resource is already there, so we'll just take the tag
                    tag = response.json()["exists"]["tag"]
                elif response.status_code != 201:
                    logger.error(
                        "couldn't hit arborist at {} to create resource (got {}): {}".format(
                            url, response.status_code, response.json()
                        )
                    )
                    raise EnvironmentError("got unexpected response from arborist")
                else:
                    # just created the resource for the first time
                    tag = response.json()["created"]["tag"]
            except (ValueError, KeyError) as e:
                raise EnvironmentError(
                    "couldn't understand response from arborist: {}".format(e)
                )
            if not tag:
                raise EnvironmentError("couldn't reach arborist")

            if self.use_arborist_tags:
                self.arborist_resources[path] = tag
                logger.info("using tag {} for path {}".format(tag, path))
                return self.arborist_resources[path]

            return path


def column_windows(session, column, windowsize, start=None):
    def int_for_range(start_id, end_id):
        if end_id:
            return and_(column >= start_id, column < end_id)
        else:
            return column >= start_id

    q = session.query(
        column, func.row_number().over(order_by=column).label("rownum")
    ).from_self(column)
    if start:
        q = q.filter(column >= start)
    if windowsize > 1:
        q = q.filter(sqlalchemy.text("rownum % {}=1".format(windowsize)))

    intervals = [id for id, in q]

    while intervals:
        start = intervals.pop(0)
        if intervals:
            end = intervals[0]
        else:
            end = None
        yield int_for_range(start, end)


def windowed_query(session, q, column, windowsize, start=None):
    for whereclause in column_windows(q.session, column, windowsize, start=start):
        for row in q.filter(whereclause).order_by(column):
            yield row
        session.commit()
        logger.info("committed progress to database")


if __name__ == "__main__":
    main()

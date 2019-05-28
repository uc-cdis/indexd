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

Furthermore, there are two ways to represent the arborist resources that go into
`authz`: the path (human-readable string) and the tag (random string, pseudo-UUID). The
tags are what we want to ultimately put into the `authz` field, since these are
persistent whereas the path could change if resources are renamed.
"""

import argparse
import sys

from cdislogging import get_logger
import requests
from sqlalchemy.engine import create_engine
from sqlalchemy.exc import OperationalError

from indexd.index.drivers.alchemy import IndexRecord, IndexRecordAuthz


logger = get_logger("migrate_acl_authz")


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
        acl_converter = ACLConverter(args.sheepdog, args.arborist)
    except EnvironmentError:
        logger.error("can't continue without database connection")
        sys.exit(1)
    with driver.session as session:
        records = session.query(IndexRecord)
        for record in records:
            if not record.acl:
                logger.info(
                    "record {} has no acl, setting authz to empty"
                    .format(record.did)
                )
                record.authz = []
                continue
            try:
                record.authz = acl_converter.acl_to_authz(record)
                session.add(record)
                logger.info(
                    "updated authz for {} to {}"
                    .format(record.did, record.authz.resource)
                )
            except EnvironmentError as e:
                msg = "problem adding authz for record {}: {}".format(record.did, e)
                logger.error(msg)
    logger.info("finished migrating")
    return


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--path", default="/var/www/indexd/", help="path to find local_settings.py",
    )
    parser.add_argument(
        "--sheepdog-db", dest="sheepdog", help="URI for the sheepdog database"
    )
    parser.add_argument(
        "--arborist-url", dest="arborist", help="URL for the arborist service"
    )
    return parser.parse_args()


class ACLConverter(object):
    def __init__(self, sheepdog_db, arborist_url):
        self.arborist_url = arborist_url.rstrip("/")
        self.programs = set()
        self.projects = dict()
        # map resource paths to tags in arborist so we can save http calls
        self.arborist_resources = dict()

        engine = create_engine(sheepdog_db, echo=False)
        try:
            connection = engine.connect()
        except OperationalError:
            raise EnvironmentError(
                "couldn't connect to sheepdog db using the provided URI"
            )

        result = connection.execute("SELECT _props->>'name' as name from node_program;")
        for row in result:
            self.programs.add(row["name"])

        result = connection.execute("""
            SELECT
                project._props->>'name' AS name,
                program._props->>'name' AS program
            FROM node_project AS project
            JOIN edge_projectmemberofprogram AS edge ON edge.src_id = project.node_id
            JOIN node_program AS program ON edge.dst_id = program.node_id;
        """)
        for row in result:
            self.projects[row["name"]] = row["program"]

        connection.close()
        return

    def is_program(self, acl_item):
        return acl_item in self.programs

    def acl_to_authz(self, record):
        path = None
        for acl_object in record.acl:
            acl_item = acl_object.ace
            if acl_item == "*":
                path = "/open"
            elif not path and self.is_program(acl_item):
                path = "/programs/{}".format(acl_item)
            else:
                if acl_item not in self.projects:
                    raise EnvironmentError(
                        "program or project {} does not exist".format(acl_item)
                    )
                path = "/programs/{}/projects/{}".format(
                    acl_item, self.projects[acl_item]
                )

        if not path:
            logger.error(
                "couldn't get `authz` for record {} from {}; setting as empty"
                .format(record.did, record.acl)
            )
            return []

        if path not in self.arborist_resources:
            url = "{}/resource/".format(self.arborist_url)
            failed = False
            try:
                resource = {"path": path}
                response = requests.post(url, timeout=5, json=resource)
            except requests.exceptions.Timeout:
                logger.error(
                    "couldn't hit arborist to look up resource (timed out): {}".format(url)
                )
                failed = True
            tag = None
            try:
                if response.status_code == 409:
                    # resource is already there, so we'll just take the tag
                    tag = response.json()["tag"]
                elif response.status_code != 201:
                    logger.error(
                        "couldn't hit arborist at {} to create resource (got {}): {}".format(
                            url, response.status_code, response.json()
                        )
                    )
                    failed = True
                else:
                    # just created the resource for the first time
                    tag = response.json()["created"]["tag"]
            except (ValueError, KeyError) as e:
                raise EnvironmentError(
                    "couldn't understand response from arborist: {}".format(e)
                )

            if failed or not tag:
                raise EnvironmentError("couldn't reach arborist")
            self.arborist_resources[path] = tag

        tag = self.arborist_resources[path]
        return [IndexRecordAuthz(did=record.did, resource=tag)]


if __name__ == "__main__":
    main()

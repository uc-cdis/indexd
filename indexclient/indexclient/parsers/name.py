import sys
import json
import argparse
import warnings

import requests

from indexclient.indexclient import errors


# DEPRECATED 11/2019 -- interacts with old `/alias/` endpoint.
# For creating aliases for indexd records, prefer using
# the `add_alias` function, which interacts with the new
# `/index/{GUID}/aliases` endpoint.
def name_record(
    host, port, name, rev, size, hashes, release, metadata, hosts, keeper, **kwargs
):
    """
    Alias a record.
    """
    warnings.warn(
        (
            "This function is deprecated. For creating aliases for indexd "
            "records, prefer using the `add_alias_for_did` function, which "
            "interacts with the new `/index/{GUID}/aliases` endpoint."
        ),
        DeprecationWarning,
    )
    resource = "http://{host}:{port}/alias/{name}".format(
        host=host, port=port, name=name
    )

    params = {"rev": rev}

    if size is not None and size < 0:
        raise ValueError("size must be non-negative")

    host_set = set(hosts)

    hash_set = set((h, v) for h, v in hashes)
    hash_dict = {h: v for h, v in hash_set}

    if len(hash_dict) < len(hash_set):
        logging.error("multiple incompatible hashes specified")

        for h in hash_dict.items():
            hash_set.remove(h)

        for h, _ in hash_set:
            logging.error("multiple values specified for {h}".format(h=h))

        raise ValueError("conflicting hashes provided")

    data = {
        "size": size,
        "hashes": hash_dict,
        "release": release,
        "metadata": metadata,
        "host_authorities": [h for h in host_set],
        "keeper_authority": keeper,
    }

    res = requests.put(resource, params=params, json=data)

    try:
        res.raise_for_status()
    except Exception as err:
        raise errors.BaseIndexError(res.status_code, res.text)

    try:
        doc = res.json()
    except ValueError as err:
        reason = json.dumps({"error": "invalid json payload returned"})
        raise errors.BaseIndexError(res.status_code, reason)

    sys.stdout.write(json.dumps(doc))


def config(parser):
    """
    Configure the name command.
    """
    parser.set_defaults(func=name_record)

    parser.add_argument("name", help="name to assign")

    parser.add_argument("rev", nargs="?", help="revision of name")

    parser.add_argument(
        "--size", default=None, type=int, help="size of underlying data"
    )

    parser.add_argument(
        "--hash",
        nargs=2,
        metavar=("TYPE", "VALUE"),
        action="append",
        dest="hashes",
        default=[],
        help="hash type and value",
    )

    parser.add_argument(
        "--release",
        choices=["public", "private", "controlled"],
        help="data release type",
    )

    parser.add_argument("--metadata", help="metadata string")

    parser.add_argument(
        "--host", action="append", dest="hosts", default=[], help="host authority"
    )

    parser.add_argument("--keeper", help="data keeper authority")

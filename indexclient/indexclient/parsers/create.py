import sys
import json
import argparse

import requests

from .errors import BaseIndexError


def create_record(host, port, form, size, urls, hashes, **kwargs):
    """
    Create a new record.
    """
    resource = "http://{host}:{port}/index/".format(host=host, port=port)

    if size < 0:
        raise ValueError("size must be non-negative")

    urls_set = set(urls)

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
        "form": form,
        "size": size,
        "urls": [u for u in urls_set],
        "hashes": hash_dict,
    }

    res = requests.post(resource, json=data)

    try:
        res.raise_for_status()
    except Exception as err:
        raise BaseIndexError(res.status_code, res.text)

    try:
        doc = res.json()
    except ValueError as err:
        reason = json.dumps({"error": "invalid json payload returned"})
        raise BaseIndexError(res.status_code, reason)

    sys.stdout.write(json.dumps(doc))


def config(parser):
    """
    Configure the create command.
    """
    parser.set_defaults(func=create_record)

    parser.add_argument(
        "form", choices=["object", "container", "multipart"], help="record format"
    )

    parser.add_argument("--size", required=True, type=int, help="size in bytes")

    parser.add_argument(
        "--hash",
        required=True,
        nargs=2,
        metavar=("TYPE", "VALUE"),
        action="append",
        dest="hashes",
        default=[],
        help="hash type and value",
    )

    parser.add_argument(
        "--url",
        metavar="URL",
        action="append",
        dest="urls",
        default=[],
        help="known URLs associated with data",
    )

#!/usr/bin/env python

import sys
import logging
import argparse

from indexd import get_app


def setup_logging(log_levels, log_stream, **kwargs):
    """
    Sets up basic logging.
    """
    logging.basicConfig(
        level=min(log_levels), stream=log_stream,
    )


def main(host, port, debug_flask=False, **kwargs):
    app = get_app()
    app.run(
        host=host, port=port, debug=debug_flask, threaded=True,
    )


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.set_defaults(log_levels=[logging.ERROR])

    parser.add_argument(
        "--debug",
        action="append_const",
        dest="log_levels",
        const=logging.DEBUG,
        help="enable debugging logs",
    )

    parser.add_argument(
        "--verbose",
        action="append_const",
        dest="log_levels",
        const=logging.INFO,
        help="enable verbose logs",
    )

    parser.add_argument(
        "--log",
        dest="log_stream",
        metavar="LOGFILE",
        type=argparse.FileType("a"),
        default=sys.stdout,
        help="target log file",
    )

    parser.add_argument(
        "--debug-flask", action="store_true", help="enable flask debugging",
    )

    parser.add_argument(
        "--host", default="localhost", help="host to server on [localhost]",
    )

    parser.add_argument(
        "--port", default=8080, type=int, help="port to server on [8080]",
    )

    args = parser.parse_args()

    setup_logging(**args.__dict__)

    logging.debug(args)

    try:
        main(**args.__dict__)
    except Exception as err:
        logging.exception(err)
        raise

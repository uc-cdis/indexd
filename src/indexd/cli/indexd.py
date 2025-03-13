import argparse
import logging

from indexd import app

logger = logging.getLogger(__name__)


def main(host, port, debug_flask=False, **kwargs):
    _app = app.get_app()
    _app.run(
        host=host,
        port=port,
        debug=debug_flask,
        threaded=True,
    )


def parse_args():
    parser = argparse.ArgumentParser()

    parser.set_defaults(log_levels=[logging.ERROR])

    parser.add_argument(
        "--verbose",
        action="append_const",
        dest="log_levels",
        const=logging.INFO,
        help="enable verbose logs",
    )

    parser.add_argument(
        "--debug-flask",
        action="store_true",
        help="enable flask debugging",
    )

    parser.add_argument(
        "--host",
        default="localhost",
        help="host to server on [localhost]",
    )

    parser.add_argument(
        "--port",
        default=8080,
        type=int,
        help="port to server on [8080]",
    )

    args = parser.parse_args()
    logging.debug(args)

    try:
        main(**args.__dict__)
    except Exception as err:
        logger.exception(err)
        raise


if __name__ == "__main__":
    parse_args()

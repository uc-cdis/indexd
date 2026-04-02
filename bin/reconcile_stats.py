"""
Util to reconcile the indexd stats table.

Recomputes record count and total bytes from the index_record table
and upserts the current month's StatsRecord row. Logs the delta.
"""

import argparse
import sys

from cdislogging import get_logger
from indexd.stats_table_migration import seed_stats

logger = get_logger(__name__, log_level="info")


def main(path):
    sys.path.append(path)
    try:
        from local_settings import settings
    except ImportError:
        logger.info("Can't import local_settings, importing from defaults")
        from indexd.default_settings import settings

    driver = settings["config"]["INDEX"]["driver"]

    with driver.session as session:
        count, total_bytes = seed_stats(session)
        session.commit()

    logger.info(
        "Reconciliation complete: record_count=%d total_bytes=%d",
        count,
        total_bytes,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Reconcile the indexd stats table from index_record data"
    )
    parser.add_argument(
        "--path",
        default="/var/www/indexd/",
        help="Path to directory containing local_settings.py (default: /var/www/indexd/)",
    )
    args = parser.parse_args()
    main(args.path)

from sqlalchemy import and_

from .index.drivers.alchemy import StatsRecord
from datetime import datetime

try:
    from .local_settings import settings
except ImportError:
    from .default_settings import settings


# Get the index driver
driver = settings["config"]["INDEX"]["driver"]


with driver.session as session:
    new_stat = StatsRecord()

    # compute new stats
    new_stat.total_record_count = driver.len()
    new_stat.total_record_bytes = driver.totalbytes()
    new_stat.month = datetime.now().month
    new_stat.year = datetime.now().year

    # Check if stats for current month/year have previously been added
    query = session.query(StatsRecord).filter(
        and_(
            StatsRecord.month == new_stat.month,
            StatsRecord.year == new_stat.year,)
    )

    existing_record = query.first()

    if existing_record == None:
        session.add(new_stat)
    else:
        existing_record.total_record_count = new_stat.total_record_count
        existing_record.total_record_bytes = new_stat.total_record_bytes

    session.commit()

from sqlalchemy import (
    BigInteger,
    Column,
)
from .index.drivers.alchemy import Base

try:
    from .local_settings import settings
except ImportError:
    from .default_settings import settings


class StatsRecord(Base):
    """
    Stats table row representation.
    """

    __tablename__ = "stats"
    total_record_count = Column(BigInteger)
    total_record_bytes = Column(BigInteger)


# Get the index driver
driver = settings["config"]["INDEX"]["driver"]


with driver.session as session:
    stat = StatsRecord()
    # compute new stats
    stat.total_record_count = driver.len()
    stat.total_record_bytes = driver.totalbytes()
    # delete any existing stats
    session.query(StatsRecord).delete()
    session.add(stat)
    session.commit()

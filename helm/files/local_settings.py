import sys
import os
sys.path.append('/var/www/indexd')

from indexd.index.drivers.alchemy import SQLAlchemyIndexDriver
from indexd.alias.drivers.alchemy import SQLAlchemyAliasDriver
from indexd.auth.drivers.alchemy import SQLAlchemyAuthDriver


URL_STRING = 'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_name}'.format(
    pg_user=os.getenv("PG_USER", ""),
    pg_pass=os.environ.get("PG_PASS"),
    pg_host=os.environ.get("PG_HOST"),
    pg_port=int(os.getenv("PG_PORT", "5432")),
    pg_name=os.getenv("PG_DATABASE", ""),
)
AUTO_MIGRATE = False

CONFIG = {
    'INDEX': {
        'driver': SQLAlchemyIndexDriver(
            URL_STRING, auto_migrate=AUTO_MIGRATE, pool_size=2, max_overflow=6,
        ),
    },
    'ALIAS': {
        'driver': SQLAlchemyAliasDriver(
            URL_STRING, auto_migrate=AUTO_MIGRATE, pool_size=2, max_overflow=6,
        ),
    },
    'JSONIFY_PRETTYPRINT_REGULAR': False,
    'PREPEND_PREFIX': False,
}

# Auth does not support auto_migrate functionality currently.
AUTH = SQLAlchemyAuthDriver(URL_STRING, pool_size=1, max_overflow=3)
settings = {'config': CONFIG, 'auth': AUTH}

from .index.drivers.alchemy import SQLAlchemyIndexDriver
from .alias.drivers.alchemy import SQLAlchemyAliasDriver
from .auth.drivers.alchemy import SQLAlchemyAuthDriver

CONFIG = {}

CONFIG['JSONIFY_PRETTYPRINT_REGULAR'] = False
AUTO_MIGRATE = True

CONFIG['INDEX'] = {
    'driver':  SQLAlchemyIndexDriver(
        'sqlite:///index.sq3', auto_migrate=AUTO_MIGRATE),
}

CONFIG['ALIAS'] = {
    'driver': SQLAlchemyAliasDriver(
        'sqlite:///alias.sq3', auto_migrate=AUTO_MIGRATE),
}

AUTH = SQLAlchemyAuthDriver('sqlite:///auth.sq3')

settings = {'config': CONFIG, 'auth': AUTH}

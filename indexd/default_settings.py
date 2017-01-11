from index.drivers.alchemy import SQLAlchemyIndexDriver
from alias.drivers.alchemy import SQLAlchemyAliasDriver
from auth.drivers.alchemy import SQLAlchemyAuthDriver

CONFIG = {}

CONFIG['JSONIFY_PRETTYPRINT_REGULAR'] = False
CONFIG['INDEX'] = {
    'driver':  SQLAlchemyIndexDriver('sqlite:///index.sq3'),
}

CONFIG['ALIAS'] = {
    'driver': SQLAlchemyAliasDriver('sqlite:///alias.sq3'),
}

AUTH = SQLAlchemyAuthDriver('sqlite:///auth.sq3').auth

settings = {'config': CONFIG, 'auth': AUTH}

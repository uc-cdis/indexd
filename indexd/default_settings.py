from .index.drivers.alchemy import SQLAlchemyIndexDriver
from .alias.drivers.alchemy import SQLAlchemyAliasDriver
from .auth.drivers.alchemy import SQLAlchemyAuthDriver

CONFIG = {}

CONFIG['JSONIFY_PRETTYPRINT_REGULAR'] = False
AUTO_MIGRATE = True

CONFIG['INDEX'] = {
    'driver':  SQLAlchemyIndexDriver(
        'sqlite:///index.sq3', auto_migrate=AUTO_MIGRATE,
        index_config={
            'DEFAULT_PREFIX': 'testprefix:', 'ADD_PREFIX_ALIAS': True,
            'PREPEND_PREFIX': True}
    ),
}

CONFIG['ALIAS'] = {
    'driver': SQLAlchemyAliasDriver(
        'sqlite:///alias.sq3', auto_migrate=AUTO_MIGRATE),
}

CONFIG['DIST'] = [
    {
        'name': 'Other IndexD',
        'host': 'https://indexd.example.io/index/',
        'hints': ['.*ROCKS.*'],
        'type': 'indexd',
    },
    {
        'name': 'DX DOI',
        'host': 'https://doi.org/',
        'hints': ['10\..*'],
        'type': 'doi',
    },
    {
        'name': 'DOS System',
        'host': 'https://example.com/api/ga4gh/dos/v1/',
        'hints': [],
        'type': 'dos',
    },
]

AUTH = SQLAlchemyAuthDriver('sqlite:///auth.sq3')

settings = {'config': CONFIG, 'auth': AUTH}



from distutils.command.config import config
from functools import cache
from flask_caching import Cache

cache = Cache(config={"CACHE_TYPE": "simple"})

import os

from indexd import get_app

os.environ["INDEXD_SETTINGS"] = "/var/www/indexd/"
application = get_app()

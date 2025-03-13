import os

from indexd import app

os.environ["INDEXD_SETTINGS"] = "/var/www/indexd/"
application = app.get_app()

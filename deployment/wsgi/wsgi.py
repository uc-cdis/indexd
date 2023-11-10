from indexd import get_app
import os

os.environ["INDEXD_SETTINGS"] = "/var/www/indexd/"
application = get_app()

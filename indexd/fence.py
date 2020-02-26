import re
import time
from urllib.parse import urlparse

from cached_property import cached_property
from cdislogging import get_logger
# from cdispyutils.config import get_value
# from cdispyutils.hmac4 import generate_aws_presigned_url
import flask
import requests

from indexd.errors import AuthError
from indexd.errors import UserError
from indexd.index.errors import NoRecordFound as IndexNoRecordFound
from indexd.drs.errors import UnexpectedError


logger = get_logger(__name__)

def get_signed_url_for_object(object_id, access_id):
    r = FenceRequest(object_id, access_id)
    signed_url = r.get_signed_url_from_fence
    return signed_url




class FenceRequest(object):

    def __init__(self, object_id, access_id):
        # self.base_url = base_url
        self.object_id = object_id
        self.access_id = access_id

    @cached_property
    def fence_server(self):
        fence_server = (
            flask.current_app.config.get("FENCE")
            or flask.current_app.config["BASE_URL"] + "/user"
        )
        return fence_server.rstrip("/")

    @cached_property
    def get_signed_url_from_fence(self):
        fence_server = (flask.current_app.config.get("PRESIGNED_URL_ENDPT")
                            or flask.current_app.config["PRESIGNED_URL_ENDPT"])
        fence_server = fence_server.rstrip("/") + "/user"
        api_url = fence_server + "/data/download/"
        url = api_url + self.object_id + "?protocol=" + self.access_id 
        try:
            req = requests.get(url)
            return req.json()
        except Exception as e:
            logger.error(
                "failed to reach fence at {0}: {1}".format(url + self.object_id, e)
            )
            raise UnexpectedError("Failed to retrieve access url")
        if res.status_code == 404:
            logger.error(
                "Not found. Fence could not find {}: {}".format(url + self.object_id, res.text)
            )
            raise IndexNoRecordFound("No document with id:{}".format(self.file_id))
        else:
            raise UnexpectedError(res.text)
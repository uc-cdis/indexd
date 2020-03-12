from cached_property import cached_property
from cdislogging import get_logger
import os

# from cdispyutils.config import get_value
# from cdispyutils.hmac4 import generate_aws_presigned_url
import flask
import requests

try:
    from local_settings import settings
except ImportError:
    from indexd.default_settings import settings

from indexd.index.errors import NoRecordFound as IndexNoRecordFound
from indexd.errors import UserError, UnexpectedError
from indexd.auth.errors import AuthError


logger = get_logger(__name__)

SUPPORTED_PROTOCOLS = ["s3", "http", "ftp", "https", "gs"]


class FenceClient(object):
    def __init__(self, url):
        self.url = url

    def get_signed_url_for_object(self, object_id, access_id):
        fence_server = self.url
        api_url = fence_server.rstrip("/") + "/data/download/"
        url = api_url + object_id
        if access_id is not None and access_id != "":
            if access_id not in SUPPORTED_PROTOCOLS:
                raise UserError(
                    "The specified protocol {} is not supported".format(access_id)
                )
            url += "?protocol=" + access_id
        headers = flask.request.headers
        if "AUTHORIZATION" not in headers:
            logger.error("Bearer Token not available.")
            raise AuthError("Not Authorized. Please Log In.")
        try:
            req = requests.get(url, headers=headers)
        except Exception as e:
            logger.error("failed to reach fence at {0}: {1}".format(url + object_id, e))
            raise UnexpectedError("Failed to retrieve access url")
        if req.status_code == 404:
            logger.error(
                "Not found. Fence could not find {}: {} with access id: {}".format(
                    url + object_id, req.text, access_id
                )
            )
            raise IndexNoRecordFound(
                "No document with id:{} with access_id:{}".format(object_id, access_id)
            )
        elif req.status_code != 200:
            raise UnexpectedError(req.text)
        return req.json()

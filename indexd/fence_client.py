from cdislogging import get_logger

import flask
import requests


from indexd.index.errors import NoRecordFound as IndexNoRecordFound
from indexd.errors import UnexpectedError
from indexd.auth.errors import AuthError, AuthzError


logger = get_logger(__name__)


class FenceClient(object):
    def __init__(self, url):
        self.url = url

    def get_signed_url_for_object(self, object_id, access_id):
        fence_server = self.url
        api_url = fence_server.rstrip("/") + "/data/download/"
        url = api_url + object_id
        headers = flask.request.headers
        if "AUTHORIZATION" not in headers:
            logger.error("Bearer Token not available.")
            raise AuthError("Not Authorized. Access Token Required.")
        if access_id:
            url += "?protocol=" + access_id
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
        if req.status_code == 401:
            raise AuthzError("Unauthorized: Access denied due to invalid credentials.")
        elif req.status_code != 200:
            raise UnexpectedError(req.text)
        return req.json()

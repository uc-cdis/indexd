import copy
import json
import warnings

import requests

from indexclient.indexclient.errors import BaseIndexError

MAX_RETRIES = 10

UPDATABLE_ATTRS = [
    "file_name",
    "urls",
    "version",
    "metadata",
    "acl",
    "authz",
    "urls_metadata",
    "description",
    "content_created_date",
    "content_updated_date",
]


def json_dumps(data):
    return json.dumps({k: v for (k, v) in data.items() if v is not None})


def handle_error(resp):
    if 400 <= resp.status_code < 600:
        try:
            json_resp = resp.json()
            resp.reason = json_resp["error"]
        except (json.decoder.JSONDecodeError, KeyError):
            pass
        finally:
            resp.raise_for_status()


def timeout_wrapper(func):
    def timeout(*args, **kwargs):
        kwargs.setdefault("timeout", 60)
        return func(*args, **kwargs)

    return timeout


def retry_and_timeout_wrapper(func):
    def retry_logic_with_timeout(*args, **kwargs):
        kwargs.setdefault("timeout", 60)
        retries = 0
        while retries < MAX_RETRIES:
            try:
                return func(*args, **kwargs)
            except requests.exceptions.ReadTimeout:
                retries += 1
                if retries == MAX_RETRIES:
                    raise

    return retry_logic_with_timeout


class IndexClient(object):
    def __init__(self, baseurl, version="v0", auth=None):
        self.auth = auth
        self.url = baseurl
        self.version = version

    def url_for(self, *path):
        subpath = "/".join(path).lstrip("/")
        return "{}/{}".format(self.url.rstrip("/"), subpath)

    def check_status(self):
        """Check that the API we are trying to communicate with is online"""
        resp = requests.get(self.url + "/index")
        handle_error(resp)

    def global_get(self, did, no_dist=False):
        """
        Makes a web request to the Indexd service global endpoint to retrieve
        an index document record.

        :param str did:
            The UUID for the index record we want to retrieve.

        :param boolean no_dist:
            *optional* Specify if we want distributed search or not

        :returns: A Document object representing the index record
        """
        try:
            if no_dist:
                response = self._get(did, params={"no_dist": ""})
            else:
                response = self._get(did)
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                return None
            else:
                raise e

        return Document(self, did, json=response.json())

    def get(self, did):
        """
        Makes a web request to the Indexd service to retrieve an index document record.

        :param str did:
            The UUID for the index record we want to retrieve.

        :returns: A Document object representing the index record
        """
        try:
            response = self._get("index", did)
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                return None
            else:
                raise e

        return Document(self, did, json=response.json())

    def bulk_request(self, dids):
        """
        bulk_get makes one http request to the indexd service and retrieves
        a list of Documents based on the dids provided.

        Args:
            dids (list): list of dids for potential documents

        Returns:
            list: Document objects representing  index records
        """

        headers = {"content-type": "application/json"}
        try:
            response = self._post("bulk/documents", json=dids, headers=headers)
        except requests.HTTPError as exception:
            if exception.response.status_code == 404:
                return None
            else:
                raise exception

        return [Document(self, doc["did"], json=doc) for doc in response.json()]

    def get_with_params(self, params=None):
        """
        Return a document object corresponding to the supplied parameters, such
        as ``{'hashes': {'md5': '...'}, 'size': '...', 'metadata': {'file_state': '...'}}``.
        """
        # need to include all the hashes in the request
        # index client like signpost or indexd will need to handle the
        # query param `'hash': 'hash_type:hash'`
        params_copy = copy.deepcopy(params) or {}
        if "hashes" in params_copy:
            params_copy["hash"] = params_copy.pop("hashes")
        reformatted_params = dict()
        for param in ["hash", "metadata"]:
            if param in params_copy:
                reformatted_params[param] = []
                for k, v in params_copy[param].items():
                    reformatted_params[param].append(str(k) + ":" + str(v))
                del params_copy[param]
        reformatted_params.update(params_copy)
        reformatted_params["limit"] = 1

        try:
            response = self._get("index", params=reformatted_params)
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                return None
            else:
                raise e
        if not response.json()["records"]:
            return None
        json = response.json()["records"][0]
        did = json["did"]
        return Document(self, did, json=json)

    def list(self, limit=float("inf"), start=None, page_size=100):
        """Returns a generator of document objects."""
        return self.list_with_params(limit, start, page_size)

    def list_with_params(
        self,
        limit=float("inf"),
        start=None,
        page_size=100,
        params=None,
        negate_params=None,
    ):
        """
        Return a generator of document object corresponding to the supplied parameters, such
        as ``{'hashes': {'md5': '...'},
              'size': '...',
              'metadata': {'file_state': '...'},
              'urls_metadata': {'s3://url': {'state': '...'}
             }``.
        """
        params_copy = copy.deepcopy(params) or {}
        if "hashes" in params_copy:
            params_copy["hash"] = params_copy.pop("hashes")
        if "urls_metadata" in params_copy:
            params_copy["urls_metadata"] = json.dumps(params_copy.pop("urls_metadata"))
        reformatted_params = dict()
        for param in ["hash", "metadata"]:
            if param in params_copy:
                reformatted_params[param] = []
                for k, v in params_copy[param].items():
                    reformatted_params[param].append(str(k) + ":" + str(v))
                del params_copy[param]
        reformatted_params.update(params_copy)
        reformatted_params.update({"limit": page_size, "start": start})
        if negate_params:
            reformatted_params.update({"negate_params": json.dumps(negate_params)})
        yielded = 0
        while True:
            resp = self._get("index", params=reformatted_params, timeout=60)
            handle_error(resp)
            json_str = resp.json()
            if not json_str["records"]:
                return
            for doc in json_str["records"]:
                if yielded < limit:
                    yield Document(self, None, json=doc)
                    yielded += 1
                else:
                    return
            if len(json_str["records"]) == page_size:
                reformatted_params["start"] = json_str["records"][-1]["did"]
            else:
                # There's no more results
                return

    def create(
        self,
        hashes,
        size,
        did=None,
        urls=None,
        file_name=None,
        metadata=None,
        baseid=None,
        acl=None,
        urls_metadata=None,
        version=None,
        authz=None,
        description=None,
        content_created_date=None,
        content_updated_date=None,
    ):
        """Create a new entry in indexd

        Args:
            hashes (dict): {hash type: hash value,}
                eg ``hashes={'md5': ab167e49d25b488939b1ede42752458b'}``
            size (int): file size metadata associated with a given uuid
            did (str): provide a UUID for the new indexd to be made
            urls (list): list of URLs where you can download the UUID
            acl (list): access control list
            authz (str): RBAC string
            file_name (str): name of the file associated with a given UUID
            metadata (dict): additional key value metadata for this entry
            urls_metadata (dict): metadata attached to each url
            baseid (str): optional baseid to group with previous entries versions
            version (str): entry version string
        Returns:
            Document: indexclient representation of an entry in indexd
        """

        if urls is None:
            urls = []
        json = {
            "urls": urls,
            "form": "object",
            "hashes": hashes,
            "size": size,
            "file_name": file_name,
            "metadata": metadata,
            "urls_metadata": urls_metadata,
            "baseid": baseid,
            "acl": acl,
            "authz": authz,
            "version": version,
            "description": description,
            "content_created_date": content_created_date,
            "content_updated_date": content_updated_date,
        }
        if did:
            json["did"] = did
        resp = self._post(
            "index/",
            headers={"content-type": "application/json"},
            data=json_dumps(json),
            auth=self.auth,
        )
        return Document(self, resp.json()["did"])

    def add_alias_for_did(self, alias, did):
        """
        Adds an alias for a document id (did). Once an alias is created for
        a did, the document can be retrieved by the alias using the
        `global_get(alias)` function.


        :param str alias:
            The alias we want to assign to the document id.
        :param str did:
            The document id for the index record we want to alias.

        :raises BaseIndexError:
            Raised if aliasing operation fails.
        """
        alias_payload = {"aliases": [{"value": alias}]}
        resp = self._post(
            "index/{}/aliases".format(did),
            headers={"content-type": "application/json"},
            data=json.dumps(alias_payload),
            auth=self.auth,
        )
        try:
            return resp.json()
        except ValueError as err:
            reason = json.dumps(
                {"error": "invalid json payload returned: {}".format(err)}
            )
            raise BaseIndexError(resp.status_code, reason)

    # DEPRECATED 11/2019 -- interacts with old `/alias/` endpoint.
    # For creating aliases for indexd records, prefer using
    # the `add_alias_for_did` function, which interacts with the new
    # `/index/{GUID}/aliases` endpoint.
    def create_alias(
        self,
        record,
        size,
        hashes,
        release=None,
        metastring=None,
        host_authorities=None,
        keeper_authority=None,
    ):
        warnings.warn(
            (
                "This function is deprecated. For creating aliases for indexd "
                "records, prefer using the `add_alias_for_did` function, which "
                "interacts with the new `/index/{GUID}/aliases` endpoint."
            ),
            DeprecationWarning,
        )
        data = json_dumps(
            {
                "size": size,
                "hashes": hashes,
                "release": release,
                "metastring": metastring,
                "host_authorities": host_authorities,
                "keeper_authority": keeper_authority,
            }
        )
        url = "alias/" + record
        headers = {"content-type": "application/json"}
        resp = self._put(url, headers=headers, data=data, auth=self.auth)
        return resp.json()

    def get_latest_version(self, did, skip_null_versions=False):
        """
        Args:
            did (str): document id of an existing entry whose latest version is requested
            skip_null_versions (bool): if True, exclude entries without a version
        Returns:
            Document: latest version of the entry
        """

        params = {"has_version": "true" if skip_null_versions else "false"}
        doc = self._get("index", did, "latest", params=params).json()

        if doc and "did" in doc:
            return Document(self, doc["did"], doc)
        return None

    def add_version(self, current_did, new_doc):
        """

        Args:
            current_did (str): did of an existing index whose baseid will be shared
            new_doc (Document): the document version to add
        Return:
            Document: the version that was just added
        """

        rev_doc = self._post(
            "index", current_did, json=new_doc.to_json(), auth=self.auth
        ).json()
        if rev_doc and "did" in rev_doc:
            return Document(self, rev_doc["did"])
        return None

    def list_versions(self, did):
        # type: (str) -> list[Document]
        versions_dict = self._get("index", did, "versions").json()  # type: dict
        versions = []

        for _, version in versions_dict.items():
            versions.append(Document(self, version["did"], version))
        return versions

    @retry_and_timeout_wrapper
    def _get(self, *path, **kwargs):
        resp = requests.get(self.url_for(*path), **kwargs)
        handle_error(resp)
        return resp

    @timeout_wrapper
    def _post(self, *path, **kwargs):
        resp = requests.post(self.url_for(*path), **kwargs)
        handle_error(resp)
        return resp

    @timeout_wrapper
    def _put(self, *path, **kwargs):
        resp = requests.put(self.url_for(*path), **kwargs)
        handle_error(resp)
        return resp

    @timeout_wrapper
    def _delete(self, *path, **kwargs):
        resp = requests.delete(self.url_for(*path), **kwargs)
        handle_error(resp)
        return resp


class DocumentDeletedError(Exception):
    pass


class Document(object):
    def __init__(self, client, did, json=None):
        self.client = client
        self.did = did
        self._fetched = False
        self._deleted = False
        self._load(json)

    def __eq__(self, other_doc):
        """
        equals `==` operator overload

        It doesn't matter the order of the urls list. What matters is the
        existence of the urls are the same on both sides.
        """
        return self._sorted_doc == other_doc._sorted_doc

    def __ne__(self, other_doc):
        """
        not equals `!=` operator overload

        It doesn't matter the order of the urls list. What matters is the
        existence of the urls are the same on both sides.
        """
        return self._sorted_doc != other_doc._sorted_doc

    def __lt__(self, other_doc):
        return self.did < other_doc.did

    def __gt__(self, other_doc):
        return self.did > other_doc.did

    def __repr__(self):
        """
        String representation of a Document

        Example:
            <Document(size=1, form=object, file_name=filename.txt, ...)>
        """
        attributes = ", ".join(
            ["{}={}".format(attr, self.__dict__[attr]) for attr in self._attrs]
        )
        return "<Document(" + attributes + ")>"

    def _check_deleted(self):
        if self._deleted:
            raise DocumentDeletedError("document {} has been deleted".format(self.did))

    def _render(self, include_rev=True):
        self._check_deleted()
        if not self._fetched:
            raise RuntimeError(
                "Document must be fetched from the server before being rendered as json"
            )
        return self._doc

    def to_json(self, include_rev=True):
        json = self._render(include_rev=include_rev)
        if self.did:
            json["did"] = self.did
        return json

    def _load(self, json=None):
        """Load the document contents from the server or from the provided dictionary"""
        self._check_deleted()
        json = json or self.client._get("index", self.did).json()
        # set attributes to current Document
        for k, v in json.items():
            self.__dict__[k] = v
        self._attrs = json.keys()
        self._fetched = True

    def _doc_for_update(self):
        """
        return document with subset of attributes that are allowed
        to be updated
        """
        return {k: v for k, v in self._doc.items() if k in UPDATABLE_ATTRS}

    @property
    def _doc(self):
        return {k: self.__dict__[k] for k in self._attrs}

    @property
    def _sorted_doc(self):
        """Return the _doc object but with all arrays in sorted order.

        This will allow us to compare dictionaries with lists correctly. We
        only care about the contents of the arrays not the order of them.
        """
        return recursive_sort(self._doc)

    def patch(self):
        """Update attributes in an indexd Document

        "Patch" the current document attributes then upload the
        changed result to the indexd server.
        """

        self._check_deleted()
        self.client._put(
            "index",
            self.did,
            params={"rev": self.rev},
            headers={"content-type": "application/json"},
            auth=self.client.auth,
            data=json.dumps(self._doc_for_update()),
        )
        self._load()  # to sync new rev from server

    def delete(self):
        self._check_deleted()
        self.client._delete(
            "index", self.did, auth=self.client.auth, params={"rev": self.rev}
        )
        self._deleted = True


def recursive_sort(value):
    """
    Sort all the lists in the dictionary recursively so that we can compare a
    dictionary's contents being the same instead of comparing their order.
    """
    if isinstance(value, dict):
        return {key: recursive_sort(value[key]) for key in value.keys()}
    elif isinstance(value, list):
        return sorted([recursive_sort(element) for element in value])
    else:
        return value

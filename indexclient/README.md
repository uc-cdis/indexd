Indexclient
===
![version](https://img.shields.io/badge/version-2.0.0-green.svg?style=flat) [![Apache license](http://img.shields.io/badge/license-Apache-blue.svg?style=flat)](LICENSE) [![Travis](https://travis-ci.org/uc-cdis/indexclient.svg?branch=master)](https://travis-ci.org/uc-cdis/indexclient)

Indexclient is a prototype data indexing and tracking client. It is intended to
provide a simple means of interactively investigating
[indexd](https://github.com/LabAdvComp/indexd) deployments. It is built upon
a basic REST-like API and demonstrates how a client utility can be built to
interact with the index in a meaningful manner.

## Installation

The prototype implementation for the client is requests based. This
provides a minimum list of requirements and allows for deployment on a wide
range of systems with next to no configuration overhead. That said, it is
highly recommended to use pip and a virtualenv to isolate the installation.

To install the prototype implementation, simply run

```bash
pip install .
```

## Configuration

At present, all configuration options are hard-coded in the prototype. This
will be subject to change in the future, as options are moved to configuration
files. Until that time, the primary hard-coded configurations to keep in
mind is the index host and port combination.

```python
HOST = 'localhost'
PORT = 8080
```

## Index Records

Records are collections of information necessary to as-uniquely-as-possible
identify a piece of information. This is done through the use of hashes and
metadata. Records are assigned a UUIDv4 at the time of creation. This allows
records to be uniquely referenced amongst multiple records. To prevent an
update conflict when multiple systems are editing the same record, a revision
is stored and changed for every update. This is an opaque string and is
not used for anything other than avoiding update conflicts.

Hashes used by the index are deployment specific, but are intended to be the
results of widely known and commonly available hashing algorithms, such as
MD5 or SHA1. This is similar to the way that torrents are tracked, and provides
a mechanism by which data can be safely retrieved from potentially untrusted
sources in a secure manner.

Additional metadata that is store in index records include the size of the
data as well as the type.

Records adhere to the json-schema described in [indexd](https://github.com/LabAdvComp/indexd/blob/master/indexd/index/schema.py#L1):


An example of one such record:

```json
{
    "id": "119d292f-b786-421e-a8dd-72208e77c269",
    "rev": "dbee8496-5d03-4fbd-9115-6871c4ebf59f",
    "size": 512,
    "hash": {
        "md5": "e2a3a55aa1596f87f502c8ff29d74244",
        "sha1": "cb4e5ba5d30fd4667beba95bf73ea9d76ad3dcd4",
        "sha256": "20b599fa98f5f98e89e128ba6de3b65ff753c662721f368649fb8d7e7d4933b0"
    },
    "type": "object",
    "urls": [
      "s3://endpointurl/bucket/key"
    ]
}
```


## Making Queries


All queries to the index are made through HTTP using JSON data payloads.
This gives a simple means of interaction that is easily accessible to any
number of languages.

These queries are handled via requests and wrapped into the index client.


### Create a record


#### Method: `create`

Example:

```python
indexclient.create(
hashes = {'md5': ab167e49d25b488939b1ede42752458b'},
size = 5,
# optional arguments
acl = ["a", "b"]
)
```


### Retrieve a record


#### Method: `get`

Example:

```python
indexclient.get("dg.1234/03eed607-acb0-4532-b0ee-9e3766b1aa6e")
```

#### Method: `global_get`

Example:

```python
indexclient.global_get("dg.1234/03eed607-acb0-4532-b0ee-9e3766b1aa6e")
```
or
```python
indexclient.global_get("dg.1234/03eed607-acb0-4532-b0ee-9e3766b1aa6e", no_dist=True)
```

`global_get` can also be used to retrieve records by alias. See [Add an alias for a record](#add-an-alias-for-a-record).
```python
# Retrieve a document by its alias, "10.1000/182"
doc = indexclient.global_get("10.1000/182")
print(doc.did) # >> "g.1234/03eed607-acb0-4532-b0ee-9e3766b1aa6e"
```

#### Method: `get_with_params`

Example:

```python
params = {
'hashes': {'md5': ab167e49d25b488939b1ede42752458b'},
'size': 5
# or any other params (metadata, acl, authz, etc.)
}
indexclient.get_with_params(params)
```


### Retrieve multiple records


#### Method: `bulk_request`

Example:

```python
dids = [
"03eed607-acb0-4532-b0ee-9e3766b1aa6e",
"15684515-15b0-4532-b0ee-9e3766b65515",
"03ee4857-acb0-4123-b0ee-9e3766bffa23",
"1258d607-acb0-4532-b0ee-9e3766bffa23"
]
indexclient.bulk_request(dids)
```

### Update a record


First: get a Document object of the desired record with one of the get methods
Second: Update any of the records updatable attributes.
  - the format to do this is: `doc.attr = value`
      - eg: `doc.file_name = new_file_name`
  - Updatable attributes are: file_name urls, version, metadata, acl, authz, urls_metadata, uploader

Lastly: Update all the local changes that were made to indexd using the
        Document patch method: doc.patch()

Example:

```python
doc = indexclient.get("dg.1234/03eed607-acb0-4532-b0ee-9e3766b1aa6e"')
# or any other get method (global_get, etc.)
doc.metadata["dummy_field"] = "dummy var"
doc.acl = ['a', 'b']
doc.version = '2'
doc.patch()
```


### Delete a record


First: get a Document object of the desired record with one of the get methods
Second: Delete the record from indexd with the delete method: `doc.delete()`
Lastly: Check if the record was deleted with: `if doc._deleted`

Example:

```python
doc = indexclient.get("dg.1234/03eed607-acb0-4532-b0ee-9e3766b1aa6e")
# or any other get method (global_get, etc.)
doc.delete()
if doc._deleted == False:
return "Record is not deleted"
```

### Add an alias for a record

You can use `indexclient` to create aliases for documents in `indexd`, which enable you to retrieve documents by the alias instead of by the Document identifier (`did` / `GUID`). Aliases can be created using `indexclient.add_alias_for_did(alias=alias, did=did)` and can be retrieved using `indexclient.global_get(alias)`.

Example:

```python
res = indexclient.add_alias_for_did("10.1000/182", "g.1234/03eed607-acb0-4532-b0ee-9e3766b1aa6e")
if res.status_code != 200:
    # alias creation failed -- handle error

doc = indexclient.global_get("10.1000/182")
print(doc.did) # >> "g.1234/03eed607-acb0-4532-b0ee-9e3766b1aa6e"
```

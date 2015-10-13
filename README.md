Indexd
===
![version](https://img.shields.io/badge/version-0.0.1-orange.svg?style=flat) [![Apache license](http://img.shields.io/badge/license-Apache-blue.svg?style=flat)](LICENSE) [![Travis](https://travis-ci.org/LabAdvComp/indexd.svg?branch=master)](https://travis-ci.org/LabAdvComp/indexd)

Indexd is a prototype data indexing and tracking service. It is intended to be
distributed, hash-based indexing service, designed to be accessed via a
REST-like API or via a client, such as the
[reference implementation](https://github.com/LabAdvComp/index).

## Installation

The prototype implementation for the index is flask and SQLite3 based. This
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
files. Until that time, the two primary hard-coded configurations to keep in
mind are the database files and the server host and port combination.

```python
INDEX_SQLITE3_DATABASE = 'index.sq3'
ALIAS_SQLITE3_DATABASE = 'alias.sq3'
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

Records adhere to the following json-schema:

```json
{
    "properties": {
        "id": {
            "type": "string",
            "pattern": "^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-4[a-fA-F0-9]{3}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$"
        },
        "rev": {"type": "string"},
        "size": {"type": "integer"},
        "hash": {
            "type": "object",
            "additionalProperties": true,
        },
        "type": { "enum": ["object", "collection", "multipart"] }
    },
    "required": ["size","hash"],
    "additionalProperties": false
}
```

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
    "type": "object"
}
```

## Making Queries

All queries to the index are made through HTTP using JSON data payloads.
This gives a simple means of interaction that is easily accessible to any
number of languages.

The following examples are all given using the curl command line utility, with
a copy of the index running on localhost on port 8080.

### Create a record

***TODO***

### Name a record

***TODO***

### Retrieve a record

***TODO***

### Update a record

***TODO***

### Delete a record

***TODO***

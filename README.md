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

## Installation with Docker

```bash
docker build --build-arg https_proxy=http://cloud-proxy:3128 --build-arg http_proxy=http://cloud-proxy:3128 -t indexd .

docker run -d --name=indexd -p 80:80 indexd
docker exec indexd python /indexd/bin/index_admin.py create --username $username --password $password
docker exec indexd python /indexd/bin/index_admin.py delete --username $username
```
To run docker with an alternative settings file:
```
docker run -d -v local_settings.py:/var/www/indexd/local_settings.py --name=indexd -p 80:80 indexd
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

### Create an index

POST /index/
Content-Type: Application/json
```
{
  "form": "object",
  "size": 123,
  "urls": ["s3://endpointurl/bucket/key"],
  "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"}
}
```

| Parameters        | Values           |
| -----:|:-----|
| form      | Can be one of 'object', 'container', 'multipart' |
| size      |  File size in bytes (commonly computed via wc -c filename) |
| urls      | URLs where the datafile is stored, can be multiple locations both internally and externally |
| hashes    |  Dictionary is a string:string datastore supporting md5, sha, sha256, sha512 hash types |

Curl example:   
```
curl http://localhost/index/ -u test:test -H "Content-type: application/json" -X POST -d '{"form": "object","size": 123,"urls": ["s3://endpointurl/bucket/key"],"hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"}}'
```

***Response***
HTTP/1.1 200 OK
```
{
  "did": "82eb97e1-7c2f-4a73-9b65-ad08ef81379e",
  "rev": "80cf1989"
}
```

| Parameters        | Values           |
| ----:|:----|
| did     | Internal UUID assigned by the index service |
| rev     | 8-character internal UUID assigned by the index service |

[Full schema for creating an index](indexd/index/schema.py)

### Update an index

PUT /index/UUID?rev=REVSTRING
Content-Type: Application/json
```
{
  "rev": "80cf1989",
  "size": 123,
  "urls": ["s3://endpointurl/bucket/key"],
  "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"}
}
```

| Parameters        | Values           |
| -----:|:-----|
| rev      | Rev string of the index you wish to update |
| size      |  File size in bytes (commonly computed via wc -c filename) |
| urls      | URLs where the datafile is stored, can be multiple locations both internally and externally |
| hashes    |  Dictionary is a string:string datastore supporting md5, sha, sha256, sha512 hash types |

Curl example:
```
curl http://localhost/index/82eb97e1-7c2f-4a73-9b65-ad08ef81379e?rev=80cf1989 -u test:test -H "Content-type: application/json" -X PUT -d '{"rev": "80cf1989","size": 123,"urls": ["s3://endpointurl/bucket/key"],"hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"}}'
```

***Response***
HTTP/1.1 200 OK
```
{
  "did": "82eb97e1-7c2f-4a73-9b65-ad08ef81379e",
  "rev": "80cf1989"
}
```

| Parameters        | Values           |
| ----:|:----|
| did     | Internal UUID assigned by the index service |
| rev     | 8-character internal UUID assigned by the index service |

[Full schema for creating an index](indexd/index/schema.py)

### Retrieve an index

GET /index/UUID   

Curl example:
```
curl http://localhost/index/82eb97e1-7c2f-4a73-9b65-ad08ef81379e
```

***Response***
HTTP/1.1 200 OK
```
{
  "did": "82eb97e1-7c2f-4a73-9b65-ad08ef81379e",
  "rev": "80cf1989"
  "form": "object",
  "size": 123,
  "urls": ["s3://endpointurl/bucket/key"],
  "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"}
}
```

| Parameters        | Values           |
| ----:|:----|
| did     | Internal UUID assigned by the index service |
| rev     | 8-character internal UUID assigned by the index service |
| form      | Can be one of 'object', 'container', 'multipart' |
| size      |  File size in bytes |
| urls      | URLs where the datafile is stored, can be multiple locations both internally and externally |
| hashes    |  Dictionary is a string:string datastore supporting md5, sha, sha256, sha512 hash types |

### Delete an index

DELETE /index/UUID?rev=REVSTRING

Curl example:
```
curl http://localhost/index/82eb97e1-7c2f-4a73-9b65-ad08ef81379e?rev=80cf1989 -u test:test -X DELETE 
```

***Response***
HTTP/1.1 200 OK

### Create an alias

PUT /alias/ALIASSTRING 
Content-Type: Application/json   
```
{
  "size": 123,
  "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"},
  "release": "public",
  "keeper_authority": "OCC",
  "host_authority": ["OCC"],
  "metadata": "gov.noaa.ncdc:C00681"
}
```

| Parameters        | Values           |
| -----:|:-----|
| size      |  File size in bytes (commonly computed via wc -c filename) |
| hashes    |  Dictionary is a string:string datastore supporting md5, sha1, sha256, sha512 hash types |
| release      | How has this data been released? Options are public, private, and controlled |
| keeper_authority  | Who is the authority keeping this metadata/index up to date? |
| host_authority | Who are the authorities hosting this data? |
| metadata | String which can reference further metdata about the dataset |

Curl example:   
```
curl "http://localhost/alias/ark:/31807/DC1-TESTARK" -u test:test -H "Content-type: application/json" -X PUT -d '{"release": "public", "keeper_authority": "OCC", "host_authority": ["OCC"], "size": 123,"urls": ["s3://endpointurl/bucket/key"],"hashes": {"md5": "b9942cf415384b27cadf1f4d2d682e5a"}}'
```

***Response***
HTTP/1.1 200 OK
```json
{
  "name": "ark:/31807/DC1-TESTARK",
  "rev": "f93a62e4"
}
```

### Update an alias

PUT /alias/ALIASSTRING?rev=REVSTRING
Content-Type: Application/json
```
{
  "size": 123,
  "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"},
  "release": "public",
  "keeper_authority": "OCC",
  "host_authority": ["OCC"],
  "metadata": "gov.noaa.ncdc:C00681"
}
```

| Parameters        | Values           |
| -----:|:-----|
| size      |  File size in bytes (commonly computed via wc -c filename) |
| hashes    |  Dictionary is a string:string datastore supporting md5, sha1, sha256, sha512 hash types |
| release      | How has this data been released? Options are public, private, and controlled |
| keeper_authority  | Who is the authority keeping this metadata/index up to date? |
| host_authority | Who are the authorities hosting this data? |
| metadata | String which can reference further metdata about the dataset |

Curl example:
```
curl "http://localhost/alias/ark:/31807/DC1-TESTARK?rev=f93a62e4" -u test:test -H "Content-type: application/json" -X PUT -d '{"release": "public", "keeper_authority": "OCC", "host_authority": ["OCC", "GDC"], "size": 123,"urls": ["s3://endpointurl/bucket/key"],"hashes": {"md5": "b9942cf415384b27cadf1f4d2d682e5a"}}'
```

***Response***
HTTP/1.1 200 OK
```
{
  "name": "ark:/31807/DC1-TESTARK",
  "rev": "00898776"
}
```

| Parameters        | Values           |
| -----:|:-----|
| name      |  The alias string you specified |
| rev    |  8-character internal UUID assigned by the index service |

### Retrieve an alias

GET /index/ALIASSTRING

Curl example:
```
curl http://localhost/alias/ark:/31807/DC1-TESTARK
```

***Response***
HTTP/1.1 200 OK
```
{
  "name": "ark:/31807/DC1-TESTARK",
  "rev": "00898776"
  "size": 123,
  "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"},
  "release": "public",
  "keeper_authority": "OCC",
  "host_authority": ["OCC"],
  "metadata": "gov.noaa.ncdc:C00681"
}
```

| Parameters        | Values           |
| -----:|:-----|
| name      |  The alias string you specified |
| rev    |  8-character internal UUID assigned by the index service |
| size      |  File size in bytes (commonly computed via wc -c filename) |
| hashes    |  Dictionary is a string:string datastore supporting md5, sha1, sha256, sha512 hash types |
| release      | How has this data been released? Options are public, private, and controlled |
| keeper_authority  | Who is the authority keeping this metadata/index up to date? |
| host_authority | Who are the authorities hosting this data? |
| metadata | String which can reference further metdata about the dataset |

### Delete an alias

DELETE /index/UUID?rev=REVSTRING

Curl example:
```
curl http://localhost/alias/ark:/31807/DC1-TESTARK?rev=00898776 -u test:test -X DELETE
```

***Response***
HTTP/1.1 200 OK

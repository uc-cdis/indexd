Indexd
===
![version](https://img.shields.io/badge/version-0.0.1-orange.svg?style=flat) [![Apache license](http://img.shields.io/badge/license-Apache-blue.svg?style=flat)](LICENSE) [![Travis](https://travis-ci.org/uc-cdis/indexd.svg?branch=master)](https://travis-ci.org/uc-cdis/indexd)

Indexd is a prototype data indexing and tracking service. It is intended to be
distributed, hash-based indexing service, designed to be accessed via a
REST-like API or via a client, such as the
[reference implementation](https://github.com/uc-cdis/indexclient).

Indexd is a two-layer system. On the bottom layer, each data object has a globally unique digital ID and hashes that map to known physical locations of the data. The second layer is `aliases` that's basically user defined human-readable identifiers that map to hashes of the bottom layer.

Digital IDs are primarily used to track the current location of data as it is moved or copied from one location to another. DID can be assigned to entities in object storage, as well as XML and JSON documents. The current location(s) of a particular datum is reflected in the URL list contained within the Digital ID. As the same datum may exist in multiple locations, there may be more than one URL associated with each Digital ID. The abilities to actually access the URL provided by Indexd is done on the client site. The client has to be able to interpret the protocol encoded in the URL. This is similar to a browser accessing HTTP and FTP transparently by having it encoded in the URL. If a client comes across a URL that it doesn’t know how to access, it can report an error and the user may have to use a different client to access that URL.

In order to avoid update conflicts for frequently updated Digital IDs, Indexd uses a versioning system similar to that utilized in distributed version control systems. Within a particular Digital ID, this mechanism is referred to as the revision. For an update to take place, both the Digital ID and the revision must match that of the current Indexd document. When any update succeeds, a new revision is generated for the Indexd document. This prevents multiple, conflicting updates from occurring.

Digital IDs are intended to be publicly readable documents, and therefore contain no information other than resource locators. However, in order to prevent unauthorized editing of Digital IDs, each Digital ID keeps an ACL list. This ACL list contains the identities of users that have write permissions for the associated Digital ID. This is analogous to DNS in that anyone has permission to read a DNS record, but only the owner of the hostname is allowed to change the IP to which it points. While not part of the current architecture design, if restricted read access becomes a requirement, additional controls may be added to the Digital ID format.

The second layer of user defined aliases are introduced to add flexibility of supporting human readable identifiers and allow referencing existing identifiers that are created in other systems.

[View in Swagger](http://petstore.swagger.io/?url=https://raw.githubusercontent.com/uc-cdis/indexd/master/openapis/swagger.yaml)

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

## Making Queries

All queries to the index are made through HTTP using JSON data payloads.
This gives a simple means of interaction that is easily accessible to any
number of languages.

The following examples are all given using the curl command line utility, with
a copy of the index running on localhost on port 8080.

### Create an index

POST /index/
Content-Type: application/json
```
{
  "form": "object",
  "size": 123,
  "file_name": "abc.txt",
  "version": "ver_123",
  "urls": ["s3://endpointurl/bucket/key"],
  "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"}
}
```

| Parameters        | Values           |
| -----:|:-----|
| form      | Can be one of 'object', 'container', 'multipart' |
| size      |  File size in bytes (commonly computed via wc -c filename) |
| file_name |  Optional file name |
| version | Optional version string |
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
  "did": "d5d9a196-f36d-4ab8-bdca-a989e0f21c00",
  "baseid": "608d4730-101f-4372-aeb2-998ae41ff8a5"
  "rev": "b9dc8220"
}
```

| Parameters        | Values           |
| ----:|:----|
| did     | Internal UUID assigned by the index service |
| baseid  | Internal UUID assigned by the index service. All versions of a record share the same baseid |
| rev     | 8-digit hex revision ID assigned by the index service |

[Full schema for creating an index](indexd/index/schema.py)

### Create an index given did

POST /index/
Content-Type: application/json
```
{
  "did": "3d313755-cbb4-4b08-899d-7bbac1f6e67d",
  "form": "object",
  "size": 123,
  "file_name": "abc.txt",
  "version": "ver_123",
  "urls": ["s3://endpointurl/bucket/key"],
  "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"}
}
```

| Parameters        | Values           |
| -----:|:-----|
| did       | Unique digital ID |
| form      | Can be one of 'object', 'container', 'multipart' |
| size      |  File size in bytes (commonly computed via wc -c filename) |
| file_name |  Optional file name |
| version | Optional version string |
| urls      | URLs where the datafile is stored, can be multiple locations both internally and externally |
| hashes    |  Dictionary is a string:string datastore supporting md5, sha, sha256, sha512 hash types |

Curl example:
```
curl http://localhost/index/ -u test:test -H "Content-type: application/json" -X POST -d '{"form": "object","size": 123,"did": "3d313755-cbb4-4b08-899d-7bbac1f6e67d", urls": ["s3://endpointurl/bucket/key"],"hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"}}'
```

***Response***
HTTP/1.1 200 OK
```
{
  "did": "3d313755-cbb4-4b08-899d-7bbac1f6e67d",
  "baseid": "703d4g20-103f-8452-a672-878vb42ef8a5"
  "rev": "c6fc83d0"
}
```

| Parameters        | Values           |
| ----:|:----|
| did     | Unique digital ID |
| baseid  | Internal UUID assigned by the index service. All versions of a record share the same baseid |
| rev     | 8-digit hex revision ID assigned by the index service |

[Full schema for creating an index](indexd/index/schema.py)

### Update an index

PUT /index/UUID?rev=REVSTRING
Content-Type: application/json
```
{
  "rev": "bc362844",
  "urls": ["s3://endpointurl/bucket/key"]}
}
```

| Parameters        | Values           |
| -----:|:-----|
| rev      | Rev string of the index you wish to update |
| urls      | URLs where the datafile is stored, can be multiple locations both internally and externally |

Curl example:
```
curl http://localhost:8080/index/1668106b-348e-47f2-bc6e-a3ed06a36055?rev=bc362844 -u test:test -H "Content-type: application/json" -X PUT -d '{"rev": "80cf1989","urls": ["s3://endpointurl/bucket/key"]}'
```

***Response***
HTTP/1.1 200 OK
```
{
  "did": "1668106b-348e-47f2-bc6e-a3ed06a36055",
  "rev": "9b7ec7b3"
}
```

| Parameters        | Values           |
| ----:|:----|
| did     | Internal UUID assigned by the index service |
| rev     | 8-digit hex revision ID assigned by the index service |

[Full schema for updating an index](indexd/index/schema.py)

### Retrieve an index

GET /index/UUID

Curl example:
```
curl http://localhost:8080/index/1668106b-348e-47f2-bc6e-a3ed06a36055
```

***Response***
HTTP/1.1 200 OK
```
{
  "did": "1668106b-348e-47f2-bc6e-a3ed06a36055",
  "baseid": "608d4730-101f-4372-aeb2-998ae41ff8a5",
  "rev": "9b7ec7b3"
  "form": "object",
  "size": 123,
  "file_name": "abc.txt",
  "version": "ver_123",
  "urls": ["s3://endpointurl/bucket/key"],
  "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"},
  "created_date": "2017-11-17T06:07:27.069524",
  "updated_date": "2017-11-17T06:07:27.069524"
}
```

| Parameters        | Values           |
| ----:|:----|
| did     | Internal UUID assigned by the index service |
| baseid  | Internal UUID assigned by the index service. All the versions of a record share the same baseid |
| rev     | 8-digit hex revision ID assigned by the index service |
| form      | Can be one of 'object', 'container', 'multipart' |
| size      |  File size in bytes |
| file_name |  Optional file name |
| version | Optional version string |
| urls      | URLs where the datafile is stored, can be multiple locations both internally and externally |
| hashes    |  Dictionary is a string:string datastore supporting md5, sha, sha256, sha512 hash types |
| created_date | File created datetime  |
| updated_date | File updated datetime  |

### Create a new record version

POST /index/<did>
Content-Type: application/json
```
{
  "form": "object",
  "size": 123,
  "file_name": "abc.txt",
  "version": "ver_123",
  "urls": ["s3://endpointurl/bucket/key"],
  "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"}
}
```

| Parameters        | Values           |
| -----:|:-----|
| form      | Can be one of 'object', 'container', 'multipart' |
| size      |  File size in bytes (commonly computed via wc -c filename) |
| file_name |  Optional file name |
| version | Optional version string |
| urls      | URLs where the datafile is stored, can be multiple locations both internally and externally |
| hashes    |  Dictionary is a string:string datastore supporting md5, sha, sha256, sha512 hash types |

Curl example:
```
curl http://localhost/index/d5d9a196-f36d-4ab8-bdca-a989e0f21c00? -u test:test -H "Content-type: application/json" -X POST -d '{"form": "object","size": 123,"file_name": "abc.txt","version": "ver_123","urls": ["s3://endpointurl/bucket/key"],"hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"}}'
```

***Response***
HTTP/1.1 200 OK
```
{
  "did": "1668106b-348e-47f2-bc6e-a3ed06a36055"
  "baseid": "608d4730-101f-4372-aeb2-998ae41ff8a5",
  "rev": "bc362844"
}
```

| Parameters        | Values           |
| ----:|:----|
| did     | Internal UUID assigned by the index service |
| baseid  | Internal UUID assigned by the index service. All versions of a record share the same baseid |
| rev     | 8-digit hex revision ID assigned by the index service |

[Full schema for creating an index version](indexd/index/schema.py)

### Retrieve the lastest version

GET /index/UUID/latest

Curl example:
```
curl http://localhost:8080/index/1668106b-348e-47f2-bc6e-a3ed06a36055/latest
```

***Response***
HTTP/1.1 200 OK
```
{
  "did": "1668106b-348e-47f2-bc6e-a3ed06a36055"
  "baseid": "608d4730-101f-4372-aeb2-998ae41ff8a5",
  "rev": "fec0ce30"
  "form": "object",
  "size": 123,
  "file_name": "abc.txt",
  "version": "ver_123",
  "urls": ["s3://endpointurl/bucket/key"],
  "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"},
  "created_date": "2017-12-05T21:02:59.05345",
  "updated_date": "2017-12-05T21:02:59.05345"
}
```

| Parameters        | Values           |
| ----:|:----|
| did     | Internal UUID assigned by the index service |
| rev     | 8-digit hex revision ID assigned by the index service |
| form      | Can be one of 'object', 'container', 'multipart' |
| size      |  File size in bytes |
| file_name |  File name |
| version | Version string |
| urls      | URLs where the datafile is stored, can be multiple locations both internally and externally |
| hashes    |  Dictionary is a string:string datastore supporting md5, sha, sha256, sha512 hash types |
| created_date  | File created datetime |
| updated_date | File updated datetime  |

### Retrieve all the versions

GET /index/UUID/versions

Curl example:
```
curl http://localhost/index/60fd9e9d-da12-45b3-b9f5-20f5ab5b6105/versions
```

***Response***
HTTP/1.1 200 OK
```
{
  "0":
    {
      "did": "e4350f7e-1b16-4f23-9332-1b3ca1ccc800",
      "baseid": "18992079-ff5c-401a-9633-d5fc6349f445",
      "form": "object",
      "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"},
      "rev": "0984a150",
      "size": 123,
      "file_name": "abc.txt",
      "version": "ver_123",
      "created_date": "2017-11-17T06:11:18.01434"
      "updated_date": "2017-11-17T06:11:18.01434",
      "urls": ["s3://endpointurl/bucket/key"]
    },
  "1":
    {
      "did": "60fd9e9d-da12-45b3-b9f5-20f5ab5b6105",
      "baseid": "18992079-ff5c-401a-9633-d5fc6349f445",
      "form": "object",
      "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"},
      "rev": "fec0ce30", "size": 123,
      "file_name": "abc.txt",
      "version": "ver_123",
      "created_date": "2017-11-17T06:07:27.069524",
      "updated_date": "2017-11-17T06:07:27.069524",
      "urls": ["s3://endpointurl/bucket/key"]
   }
}
```

| Parameters        | Values           |
| ----:|:----|
| did     | Internal UUID assigned by the index service |
| baseid     | Internal UUID assigned by the index service. All the versions of a record share the same baseid |
| rev     | 8-digit hex revision ID assigned by the index service |
| form      | Can be one of 'object', 'container', 'multipart' |
| size      |  File size in bytes |
| file_name |  File name |
| version | Version string |
| urls      | URLs where the datafile is stored, can be multiple locations both internally and externally |
| hashes    |  Dictionary is a string:string datastore supporting md5, sha, sha256, sha512 hash types |
| created_date | File created datetime  |
| updated_date | File updated datetime  |


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
Content-Type: application/json
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

[Full schema for creating an alias](indexd/alias/schema.py)

### Update an alias

PUT /alias/ALIASSTRING?rev=REVSTRING
Content-Type: application/json
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
| rev    |  8-digit hex revision ID assigned by the alias service |

[Full schema for updating an alias](indexd/alias/schema.py)

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
| rev    | 8-digit hex revision ID assigned by the alias service |
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

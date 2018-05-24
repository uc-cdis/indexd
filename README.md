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

## Documentation

[View in Swagger](http://petstore.swagger.io/?url=https://raw.githubusercontent.com/uc-cdis/indexd/master/openapis/swagger.yaml)

## Installation

The prototype implementation for the index is flask and SQLite3 based. This
provides a minimum list of requirements and allows for deployment on a wide
range of systems with next to no configuration overhead. That said, it is
highly recommended to use pip and a virtualenv to isolate the installation.

To install the prototype implementation, simply run

```bash
python setup.py install
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

## Testing
- Follow [installation](#installation)
- Install [swagger-codegen](https://swagger.io/swagger-codegen/)
- Run:
```
pip install -r test-requirements.txt
swagger-codegen generate -i openapis/swagger.yaml -l python -o swagger_client
cd swagger_client; python setup.py develop; cd -
py.test -v tests/
```

## Testing with Docker

Doesn't work with all the DB tests yet, but you can adjust to run specific tests as necessary.

```
docker build -t indexd -f TestDockerfile .
```

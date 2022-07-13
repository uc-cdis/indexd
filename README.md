Indexd
===
![version](https://img.shields.io/github/release/uc-cdis/indexd.svg) [![Apache license](http://img.shields.io/badge/license-Apache-blue.svg?style=flat)](LICENSE) [![Travis](https://travis-ci.org/uc-cdis/indexd.svg?branch=master)](https://travis-ci.org/uc-cdis/indexd)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)

Indexd is a data indexing and tracking service. It is intended to be
distributed, hash-based indexing service, designed to be accessed via a
REST-like API or via a client, such as the
[reference implementation](https://github.com/uc-cdis/indexclient).

Indexd is a two-layer system. On the bottom layer, each data object has a globally unique digital ID and hashes that map to known physical locations of the data. The second layer is `aliases` that's basically user defined human-readable identifiers that map to hashes of the bottom layer.

Digital IDs are primarily used to track the current location of data as it is moved or copied from one location to another. DID can be assigned to entities in object storage, as well as XML and JSON documents. The current location(s) of a particular datum is reflected in the URL list contained within the Digital ID. As the same datum may exist in multiple locations, there may be more than one URL associated with each Digital ID. The abilities to actually access the URL provided by Indexd is done on the client site. The client has to be able to interpret the protocol encoded in the URL. This is similar to a browser accessing HTTP and FTP transparently by having it encoded in the URL. If a client comes across a URL that it doesn’t know how to access, it can report an error and the user may have to use a different client to access that URL.

In order to avoid update conflicts for frequently updated Digital IDs, Indexd uses a versioning system similar to that utilized in distributed version control systems. Within a particular Digital ID, this mechanism is referred to as the revision. For an update to take place, both the Digital ID and the revision must match that of the current Indexd document. When any update succeeds, a new revision is generated for the Indexd document. This prevents multiple, conflicting updates from occurring.

Digital IDs are intended to be publicly readable documents, and therefore contain no information other than resource locators. However, in order to prevent unauthorized editing of Digital IDs, each Digital ID keeps an ACL list. This ACL list contains the identities of users that have write permissions for the associated Digital ID. This is analogous to DNS in that anyone has permission to read a DNS record, but only the owner of the hostname is allowed to change the IP to which it points. While not part of the current architecture design, if restricted read access becomes a requirement, additional controls may be added to the Digital ID format.

The second layer of user defined aliases are introduced to add flexibility of supporting human readable identifiers and allow referencing existing identifiers that are created in other systems.


- [Indexd](#indexd)
  - [Use Cases For Indexing Data](#use-cases-for-indexing-data)
  - [Documentation](#documentation)
  - [Installation](#installation)
  - [Installation with Docker](#installation-with-docker)
  - [Configuration](#configuration)
  - [Index Records](#index-records)
  - [Testing](#testing)
  - [Testing with Docker](#testing-with-docker)
  - [Setup pre-commit hook to check for secrets](#setup-pre-commit-hook-to-check-for-secrets)


## Use Cases For Indexing Data

Data may be loaded into Indexd through a few different means supporting different use cases.

1. Index creation through Sheepdog.

When data files are submitted to a Gen3 data commons using Sheepdog, the files are automatically indexed into indexd. Sheepdog checks if the file being submitted has a hash & file size that match anything currently in indexd and if so uses the returned document GUID as the object ID reference. If no match is found in Indexd then a new record is created and stored in Indexd.

2. Indexing files on creation in object storage.

Using AWS SNS or Google PubSub it is possible to have streaming notifications when files are created, modified or deleted in the respective cloud object storage services (S3, GCS). It is then possible to use an AWS Lambda or GCP Cloud Function to automatically index the new object into Indexd. This may require using the batch processing services on AWS if the file is large to compute the necessary minimal set of hashes to support indexing. This feature can be set up on a per commons basis for any buckets of interest. The buckets do not have to be owned by the commons, but permissions to read the bucket objects and permissions for SNS or PubSub are necessary.

For existing data in buckets, the SNS or PubSub notifications may be simulated such that the indexing functions are started for each object in the bucket. This is useful because only a single code path is necessary for indexing the contents of an object.

3. Indexing void object for fully control the bucket structure.

Indexd supports void or blank records that allows users to pre-register data files in indexd before actually registering them. The complete flow contains three main steps: pre-register, hash/size/url populating and data node registration:
- Fence requests blank object from indexd. Indexd creates an object with no hash, size or urls, only the `uploader` and optionally `file_name` fields.
- Indexd listener monitors bucket update, update to indexd with url, hash, size.
- The client application (windmill or gen3-data-client) lists records for data files which the user needs to submit to the graph. The user fills all empty fields and submit the request to indexd to update the `acl`.

See docs on data upload flow for further details:
https://github.com/uc-cdis/cdis-wiki/tree/master/dev/gen3/data_upload

4. Using the Indexd REST API for record insertion.

In rare cases, it may be necessary to interact directly with the Indexd API in order to create index records. This would be necessary if users are loading data into a data commons in non-standard ways or not utilizing Sheepdog as part of their data commons.

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
pip install -r dev-requirements.txt -r requirements.txt
swagger-codegen generate -i openapis/swagger.yaml -l python -o swagger_client
cd swagger_client; python setup.py develop; cd -
py.test -v tests/

```
### MacOS
If building psycopg2 fails during install, try the following:
```bash
brew install openssl
export LDFLAGS="-L/usr/local/opt/openssl/lib"
export CPPFLAGS="-I/usr/local/opt/openssl/include"
```

## Testing with Docker

Doesn't work with all the DB tests yet, but you can adjust to run specific tests as necessary.

```
docker build -t indexd -f TestDockerfile .
```

    
## Setup pre-commit hook to check for secrets

We use [pre-commit](https://pre-commit.com/) to setup pre-commit hooks for this repo.
We use [detect-secrets](https://github.com/Yelp/detect-secrets) to search for secrets being committed into the repo. 

To install the pre-commit hook, run
```
pre-commit install
```

To update the .secrets.baseline file run
```
detect-secrets scan --update .secrets.baseline
```

`.secrets.baseline` contains all the string that were caught by detect-secrets but are not stored in plain text. Audit the baseline to view the secrets . 

```
detect-secrets audit .secrets.baseline
```



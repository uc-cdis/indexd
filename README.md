<div>
<img align="left" src="./docs/indexD.svg" alt="Indexd Logo" hspace="20"/>
</div>

# Indexd

![version](https://img.shields.io/github/release/uc-cdis/indexd.svg) [![Apache license](http://img.shields.io/badge/license-Apache-blue.svg?style=flat)](LICENSE) [![Travis](https://travis-ci.org/uc-cdis/indexd.svg?branch=master)](https://travis-ci.org/uc-cdis/indexd) [![Coverage Status](https://coveralls.io/repos/github/uc-cdis/indexd/badge.svg?branch=master)](https://coveralls.io/github/uc-cdis/indexd?branch=master)

Indexd is a hash-based data indexing and tracking service. It is designed to be accessed via a REST-like API or via a client, such as the [reference client implementation](https://github.com/uc-cdis/indexclient). It supports distributed resolution with a central resolver talking to other Indexd servers.

For more about GUIDs and an example of a central resolver, see [dataguids.org](https://dataguids.org).

> Indexd is vital microservice used in the [Gen3 Software Platform](https://gen3.org). Gen3 is an open-source platform for developing Data Commons that accelerate scientific discovery.

## The Problem That Indexd Solves

Data inevitably moves and changes, which leads to unreproducible research. It's not uncommon for physical data to be moved from one storage location for another, for domain names to change, and/or for data to exist in multiple locations.

If you run an analysis over a set of data and later it gets moved, your analysis is no longer repeatable. The same data still exists, it just isn't where you thought.

This presents a huge problem for repeatable research. There needs to be a unique identifier for a given piece of data that can be used in analyses without "hard-coding" the physical location of the data. The problem is that data moves.

<div>
<img align="right" src="./docs/guid.png" alt="GUID Example" height="250" hspace="10"/>
</div>

## The Solution: Indexd's Globally Unique Identifiers (GUIDs)

Indexd serves as an abstraction over the physical data locations, providing a Globally Unique Identifier (GUID) per datum. These identifiers will always be resolvable with Indexd and will always provide the locations of the physical data, even if the data moves.

GUIDs provide a domain-neutral, persistent way to track data across platforms. Indexd is a proven solution to provide GUIDs for data.

---

## Indexd Technical Details

* [View API Documentation](http://petstore.swagger.io/?url=https://raw.githubusercontent.com/uc-cdis/Indexd/master/openapis/swagger.yaml)
* [Skip to installation instructions](#nstallation)

Indexd is a two-layer system. On the bottom layer, each data object has a GUID and hashes that map to known physical locations of the data.

The second layer is aliases. **Aliases** are user-defined, human-readable identifiers that map to hashes of the bottom layer. This adds the flexibility of supporting human-readable identifiers and allow referencing existing identifiers that are created in other systems.

GUIDs are primarily used to track the current location of data as it is moved or copied from one location to another. GUIDs can be assigned to entities in object storage, as well as XML and JSON documents. The current location(s) of a particular datum is reflected in the URL list contained within Indexd.

As the same datum may exist in multiple locations, there may be more than one URL associated with each GUID. The ability to actually access the URL provided by Indexd is done on the client site.

> Clients must provide capabilities to access URLs specified in Indexd. Gen3 Auth (specifically the [Fence](https://github.com/uc-cdis/fence) service) is capable of creating signed URLs for accessing data.

The client has to be able to interpret the protocol encoded in the URL. This is similar to a browser accessing HTTP and FTP transparently by having it encoded in the URL. If a client comes across a URL that it doesn’t know how to access, it can report an error and the user may have to use a different client to access that URL.

All the information about a specific datum mentioned above (the GUID, URLs, hashes, file size, access control, etc.) are bundled together and referred to internally as an **Indexd record**.

> NOTE: The property of an Indexd record representing the GUID is `did` (or digital identifier)

### Indexd Records

Records are collections of information necessary to as-uniquely-as-possible identify a piece of information. This is done through the use of hashes and metadata. Records are assigned a UUIDv4 at the time of creation and additionally may include a prefix to aide in resolution (these combined become the GUID). This allows records to be uniquely referenced amongst multiple records.

Hashes used by the index are deployment-specific but are intended to be the results of widely known and commonly available hashing algorithms, such as MD5 or SHA1. This is similar to the way that torrents are tracked and provides a mechanism by which data can be safely retrieved from potentially untrusted sources in a secure manner.

Additional metadata that is stored in index records includes the size of the data as well as the type.

### Avoiding Conflicts on Updates

In order to avoid update conflicts for frequently updated GUIDs, Indexd uses a revisioning system similar to that utilized in distributed version control systems. Within a particular GUID, this mechanism is referred to as the **revision** or **rev**.

For an update to take place, both the GUID and the revision must match that of the current Indexd record. When any update succeeds, a new revision is generated for the Indexd record. This prevents multiple, conflicting updates from occurring. The revision is an opaque string and is
not used for anything other than avoiding update conflicts.

### Data Version Control

It is possible that specific data needs to be updated, but should still be logically related to previous versions of that data. It may also be the case that there were errors in previous data that are corrected in future versions.

It is still true, however, that **GUIDs should be persistent and the data they point to should be immutable**. Meaning that a GUID will always refer to the same data. The idea of a new version requires a _new_ GUID for that data (if the hash and file size have changed).

> The question is: how do you maintain a logical linking between different versions or updates for the same data?

To handle this versioning in Indexd, the concept of a `baseid` is introduced. The `baseid` is a UUID that all versions of the data (in other words, all GUIDs) point to. The `baseid` logically groups the "same" data.

It is then possible (via the API) to retrieve all versions for a given GUID. In addition, it is possible to ask for the _latest_ version of a GUID. See the [API documentation](http://petstore.swagger.io/?url=https://raw.githubusercontent.com/uc-cdis/Indexd/master/openapis/swagger.yaml) for more details.

But to reiterate, a given GUID will always point to the same data, even if there are later versions. The later versions will have _different_ GUIDs, though be connected through a common `baseid`. The Indexd API makes it possible to programmatically determine if newer versions of a given datum exist.

### Access Control

Indexd records (identified by GUIDs) are intended to be publicly readable documents, and therefore contain no information other than resource locators. However, in order to prevent unauthorized
creation/updating/deleting of records, each record keeps a list of authorization rules (in an `authz` property).

The `authz` property contains a list of abstract "resources" a user must have access to in order to have permission to update/delete the associated GUID. For backward compatibility, the ACL list that was used for access control is still available (the `acl` field).

If Indexd is used with other Gen3 software, specifically the services related to Gen3 Auth ([Fence](https://github.com/uc-cdis/fence) and [Arborist](https://github.com/uc-cdis/arborist)), it enables a more useful and robust access control system that exposes various data access methods securely by utilizing the `authz` field in Indexd.

The additional usage of the Gen3 Auth services will enable data access through signed URLs, with authorization checks based on the `authz` field in Indexd.

### Distributed Resolution: Utilizing Prefixes in GUIDs

Indexd's distributed resolution logic for a given GUID/alias is roughly as follows:

1. Attempt to get a local record with given input (as GUID)
2. Attempt to get a local record with given input (as alias)
3. Attempt distributed resolution using connected services configured in Indexd's `DIST` config
  * It is possible to resolve to a service that is *not* another Indexd, provided that a sufficient client is written to convert from the existing format to the format Indexd expects
    * Currently we have a [DOI Client](https://github.com/uc-cdis/doiclient) and [GA4GH's DOS Client](https://github.com/uc-cdis/dosclient)
  * The distributed resolution can be "smart", in that you can configure `hints` that tell a central resolver Indexd that a given input should be resolved with a specific distributed service
    * The `hints` are a list of regexes that will attempt to match against given input
    * For example: `hints: ["10\..*"]` for DOIs since they'll begin with `10.`

An example configuration (see [configuration section](#configuration) for more info) for an external service to resolve to:

```python
CONFIG["DIST"] = [
    {"name": "DX DOI", "host": "https://doi.org/", "hints": ["10\..*"], "type": "doi"},
]
```

The `type` tells Indexd which client to use for that external service. In this case, `doi` maps to the [DOI Client](https://github.com/uc-cdis/doiclient).

Indexd itself can be configured to append a prefix to the typical UUID in order to aide in the distributed resolution capabilities mentioned above. Specifically, we can add a prefix such as `dg.4GH5/` which may represent one instance of Indexd. For distributed resolution purposes, we can then create `hints` that let the central resolver know where to go when it receives a GUID with a prefix of `dg.4GH5/`.

The prefix that a given Indexd instance uses is specified in the `DEFAULT_PREFIX` configuration in the settings file. In order to ensure that this gets used and aliases get created, specify `PREPEND_PREFIX` to `True` and `ADD_PREFIX_ALIAS` to `True` as well.

## Use Cases For Indexing Data

Data may be loaded into Indexd through a few different means:

### I want to upload data to storage location(s) and index at the same time

Using the [gen3-client](https://gen3.org/resources/user/gen3-client/) you can upload objects to storage locations and mint GUIDs at the same time.

![alt text](./docs/indexd_client_upload.png "gen3-client Data Upload")

#### Blank Record Creation in Indexd

Indexd supports void or blank records that allow users to pre-register data files through Fence before actually registering them. This enables the [Data Upload flow](https://gen3.org/resources/user/submit-data/#2-upload-data-files-to-object-storage) that allows users to use a client to create Indexd records before the physical file exists in storage buckets. The complete flow contains three main steps:

1) pre-register
2) hash/size/URL populating
3) data node registration

General flow:

- Fence requests blank object from Indexd. Indexd creates an object with no hash, size or URLs, only the `uploader` and optionally `file_name` fields.
- Indexd listener monitors bucket update, updates Indexd with URL, hash, size.
- The client application (windmill or gen3-data-client) lists records for data files which the user needs to submit to the graph. The user fills all empty fields and submits the request to Indexd to update the `authz` or `acl`.

### I want to associate Indexd data to structured data in a Gen3 Data Commons

> NOTE: This assumes that the data already exists in storage location(s)

#### Indexd Record Creation Through Gen3's Data Submission Service: [Sheepdog](https://github.com/uc-cdis/sheepdog)

When data files are submitted to a Gen3 Data Commons using Sheepdog, the files are automatically indexed into Indexd. Submissions to Sheepdog can include `object_id`'s that map to existing Indexd GUIDs. Or, if there are no existing records, Sheepdog can create them on the fly.

To create Indexd records on the fly, Sheepdog will check if the file being submitted has a hash & file size matching anything currently in Indexd and if so uses the returned document GUID as the object ID reference. If no match is found in Indexd then a new record is created and stored in Indexd.

### I want to index data that is dynamically added to storage location(s)

#### Automatically Creating Indexd Records when Objects are Added to Object Storage

Using AWS SNS or Google PubSub it is possible to have streaming notifications when files are created, modified or deleted in the respective cloud object storage services (S3, GCS). It is then possible to use an AWS Lambda or GCP Cloud Function to automatically index the new object into Indexd.

> NOTE: This may require using the batch processing services if the file is large (to compute the necessary minimal set of hashes to support indexing). There are known limitations with AWS Lambda and GCP Cloud Functions related to how long a process can run before AWS/Google cuts it off. Some hash calculations may exceed that time limit.

This feature can be set up on a per Data Commons basis for any buckets of interest. The buckets do not have to be owned by the commons, but permissions to read the bucket objects and permissions for SNS or PubSub are necessary.

For existing data in buckets, the SNS or PubSub notifications may be simulated such that the indexing functions are started for each object in the bucket. This is useful because only a single code path is necessary for indexing the contents of an object.

## Indexd REST API for Record Creation

It is also possible to interact directly with the Indexd API in order to create index records. There are two options for authorization for these sorts of updates.

1) Use Basic Auth (username/password) to provide administrative control over indexd

You can use the `/bin/indexd_admin.py` to add a new username and password combination to Indexd.

_and/or_

2) Use the Gen3 Auth services ([Fence](https://github.com/uc-cdis/fence) and [Arborist](https://github.com/uc-cdis/arborist)) to control access based on access tokens provided in requests

Similar to other Gen3 services, user's must pass along their Access Token in the form of a JWT in the `Authorization` header of their request to the Indexd API. Indexd will check that the user is authorized for the items in the `authz` field by passing along your token and the action you're trying to do to the [Arborist](https://github.com/uc-cdis/arborist) service.

---

## Installation

The implementation for Indexd utilizes the Flask web framework and (by default) a SQLite3 database. This provides a minimum list of requirements and allows for deployment on a wide range of systems with next to no configuration overhead. That said, it is highly recommended to use pip and a virtualenv to isolate the installation.

To install the implementation, simply run:

```bash
python setup.py install
```

To see how the automated tests (run in Travis CI) install Indexd, check out the `.travis.yml` file in the root directory of this repository.

## Installation with Docker

```bash
docker build --build-arg https_proxy=http://cloud-proxy:3128 --build-arg http_proxy=http://cloud-proxy:3128 -t Indexd .

docker run -d --name=Indexd -p 80:80 Indexd
docker exec Indexd python /Indexd/bin/index_admin.py create --username $username --password $password
docker exec Indexd python /Indexd/bin/index_admin.py delete --username $username
```

To run docker with an alternative settings file:

```
docker run -d -v local_settings.py:/var/www/Indexd/local_settings.py --name=Indexd -p 80:80 Indexd
```

## Configuration

There is a `/indexd/default_settings.py` file which houses, you guessed it, default configuration settings. If you want to provide an alternative configuration to override these, you must supply a `local_settings.py` in the same directory as the default settings. It must contain all the same configurations from the `default_settings.py`, though may have different values.

This works because on app startup, Indexd will attempt to include a `local_settings` python module (the attempted import happens in the `/indexd/app.py` file). If a local settings file is not found, Indexd falls back on the default settings.

There is specific information about some configuration options in the [distributed resolution](#Distributed Resolution: Utilizing Prefixes in GUIDs) section of this document.

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

> NOTE: If you experience issues, you can check the `.travis.yml` file in this repo to see how swagger codegen (and which version) is being used for automated unit testing in Travis CI.

## Testing with Docker

Doesn't work with all the DB tests yet, but you can adjust to run specific tests as necessary.

```
docker build -t Indexd -f TestDockerfile .
```


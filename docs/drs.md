# Data Repository Service

Data Repository Service (DRS) API provides a generic interface to data repositories so that data can be accessed in a single, standardized way regardless of where it's stored  and how it's managed. DRS v1 supports two data types, *blob* and *bundles*. Detailed information on DRS [here](https://github.com/ga4gh/data-repository-service-schemas).
* *Blob* is like a file, a single blob of bytes, without `contents_array`.
* *Bundle* is like a folder structure that can contain `DrsObject` (either blobs or bundle) insisde `contents_array`.

NOTE: Indexd records automatically exists as a `DrsObject`

# Fetching DRS Objects with Indexd
**Quick Links:**
* [View DRS API Documentation](http://petstore.swagger.io/?url=https://raw.githubusercontent.com/uc-cdis/Indexd/master/openapis/swagger.yaml#/DRS)
* [GA4GH DRS API Documentation](https://ga4gh.github.io/data-repository-service-schemas/swagger-ui/#/DataRepositoryService/)
* [Definitions and details for field names](https://ga4gh.github.io/data-repository-service-schemas/docs/#_drsobject).

The DRS API supports a variety of different content acccess policies, depending on what `AccessMethod` records they return.

1. public content:
    - server provides an `access_url` with a url and no headers
    - caller fetches the object bytes without providing any auth info
2. private content that requires the caller to have out-of-band auth knowledge (e.g. service account credentials):
    - server provides an `access_url` with a url and no headers
    - caller fetches the object bytes, passing the auth info they obtained out-of-band
3. private content that requires the caller to pass an Authorization token:
    - server provides an `access_url` with a url and headers
    - caller fetches the object bytes, passing auth info via the specified header(s)
4. private content that uses an expensive-to-generate auth mechanism (e.g. a signed URL):
    - server provides an `access_id`
    - caller passes the access_id to the `/access` endpoint
    - server provides an `access_url` with the generated mechanism (e.g. a signed URL in the url field)
    - caller fetches the object bytes from the url (passing auth info from the specified headers, if any)

**Example Blob DrsObject:**
```javascript
{
    "access_methods": [
        {
            "access_id": "gs",
            "access_url": {
                "url": "gs://some-bucket/File-A"
            },
            "region": "",
            "type": "gs"
        },
        {
            "access_id": "s3",
            "access_url": {
                "url": "s3:/some-bucket/File-A"
            },
            "region": "",
            "type": "s3"
        }
    ],
    "aliases": [],
    "checksums": [
        {
            "checksum": "c29b922795e05b819d6d27064e636468",
            "type": "md5"
        }
    ],
    "contents": [],
    "created_time": "2020-06-22T20:34:06.136066",
    "description": null,
    "form": "object",
    "id": "dg.xxxx/01c3e7b2-2aca-47fc-b2e2-a5d7196652a5",
    "mime_type": "application/json",
    "name": "File-A",
    "self_uri": "drs://binamb.planx-pla.net/dg.xxxx/01c3e7b2-2aca-47fc-b2e2-a5d7196652a5",
    "size": 90,
    "updated_time": "2020-06-22T20:34:06.136078",
    "version": "838ed2d4"
}
```

**Example Bundle DrsObject when `expand=false`:**
```javascript
{
    "aliases": [],
    "checksums": [
        {
            "checksum": "3de2e595340a95c0e8a388bba817d8fd",
            "type": "md5"
        }
    ],
    "contents": [],
    "created_time": "2020-06-22T20:39:02.578005",
    "description": "",
    "form": "bundle",
    "id": "1a53681b-e50b-4bbc-8f99-d3af532909ec",
    "mime_type": "application/json",
    "name": "Bundle-A",
    "self_uri": "drs://binamb.planx-pla.net/1a53681b-e50b-4bbc-8f99-d3af532909ec",
    "size": 360,
    "updated_time": "2020-06-22T20:39:02.578012",
    "version": ""
}
```

**Example Bundle DrsObject when `expand=true`:**
```javascript
{
    "aliases": [],
    "checksums": [
        {
        "checksum": "3de2e595340a95c0e8a388bba817d8fd",
        "type": "md5"
        }
    ],
    "contents": [
        {
            "contents": [],
            "drs_uri": "drs://binamb.planx-pla.net/dg.xxxx/01c3e7b2-2aca-47fc-b2e2-a5d7196652a5",
            "id": "dg.xxxx/01c3e7b2-2aca-47fc-b2e2-a5d7196652a5",
            "name": "File-A"
        },
        {
            "drs_uri": "drs://binamb.planx-pla.net/dg.xxxx/050defbd-f07a-4f74-849c-3f13509a703f",
            "id": "dg.xxxx/050defbd-f07a-4f74-849c-3f13509a703f",
            "name": "Bundle-B"
            "contents": [
                {
                   "contents": [],
                    "drs_uri": "drs://binamb.planx-pla.net/dg.xxxx/0ad17b64-84b2-4c87-9691-8f6ba016b8cf",
                    "id": "dg.xxxx/0ad17b64-84b2-4c87-9691-8f6ba016b8cf",
                    "name": "File-B"
                },
                {
                    "contents": [],
                    "drs_uri": "drs://binamb.planx-pla.net/dg.xxxx/0b3f06b8-b2df-49bc-a0cf-6f77fa9c6faf",
                    "id": "dg.xxxx/0b3f06b8-b2df-49bc-a0cf-6f77fa9c6faf",
                    "name": "File-C"
                }
            ],
        },
    ],
    "created_time": "2020-06-22T20:39:02.578005",
    "description": "",
    "form": "bundle",
    "id": "1a53681b-e50b-4bbc-8f99-d3af532909ec",
    "mime_type": "application/json",
    "name": "Bundle-A",
    "self_uri": "drs://binamb.planx-pla.net/1a53681b-e50b-4bbc-8f99-d3af532909ec",
    "size": 360,
    "updated_time": "2020-06-22T20:39:02.578012",
    "version": ""
}

````

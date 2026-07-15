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

## Configuring access_method metadata
1. `cloud` is inferred from the protocol for each url defined for the DrsObject. `CLOUD_PROVIDER_MAP` environment variable can be used to detail the mapping of protocol and domain to the cloud provider.
2. `region` is determined by the region configured in fence for `S3_BUCKETS` and `GS_BUCKETS` for the bucket within each url for the DrsObjects. These can also be provided as urls_metadata for url.
3. `available` is determined by the urls_metadata for each url. Defaults to `true`.
4. `authorizations` are determined by the `DRS_AUTHORIZATION_METADATA` environment variable. This maps the IndexD record's `authz` to the issuers and supported types. If this mapping is not provided, the `DEFAULT_BEARER_ISSUER` will be used along with the `DEFAULT_PASSPORT_ISSUER` if provided. In gen3-helm, if `DEFAULT_BEARER_ISSUER` is not provided, it will default to the fence token issuer.

**Example CLOUD_PROVIDER_MAP**
```json
{
   "s3": "aws",
   "gs": "gcp",
   "az":  "azure",
   "https": {
       "m3.aicommons.com/ai": "aws",
       "storage.googleapis.com": "gcp"
   }
}
```

**Example urls_metadata**
Below is an example for using urls_metadata to provide cloud, region and available manually for a URL of a DrsObject
```json
  "urls_metadata": {
    "https://m3.aicommons.com/ai": {
      "region": "us-east-1",
      "cloud": "aws",
      "available": false
    }
  }
```

**Example of DRS_AUTHORIZATION_METADATA**
Below is an example of how to configure the issuer information for a given `authz` resource. This will apply to all DrsObjects
where an underyling IndexD record's `authz` matches the resources listed in this map.

If an IndexD record's `authz` is not listed, the `DEFAULT_BEARER_ISSUER` and `DEFAULT_PASSPORT_ISSUER` will be used.
```json
{
    "/gen3/programs/a/projects/b": {
        "supported_types": ["BearerAuth", "PassportAuth"],
        "passport_auth_issuers": [
            "https://ras/foo/bar",
            "https://ras/foo/bar",
            "https://ras/foo/bar/bar"
        ],
        "bearer_auth_issuers": [
            "https://gen3.datacommons.io",
            "https://gen3.datacommons.io",
            "sample_url"
        ]
    },
    "/gen3/programs/c/projects/d": {
        "supported_types": ["BearerAuth", "PassportAuth"],
        "passport_auth_issuers": [
            "sample_url_c_one",
            "sample_url_c_one",
            "sample_url_c_two"
        ],
        "bearer_auth_issuers": [
            "sample_url_d_one",
            "sample_url_d_one",
            "sample_url_d_two"
        ]
    }
}
```

**Example Blob DrsObject:**
```javascript
{
  "access_methods": [
    {
      "access_id": "s3",
      "access_url": {
        "url": "s3://cdis-presigned-url-test/testdata"
      },
      "authorizations": {
        "bearer_auth_issuers": [
          "https://gen3.biodatacatalyst.nhlbi.nih.gov/user"
        ],
        "drs_object_id": "dg.4503/2ea50456-60f6-4b4c-92cc-5fc6776343ac",
        "passport_auth_issuers": [
          "https://stsstg.nih.gov"
        ],
        "supported_types": [
          "BearerAuth",
          "PassportAuth"
        ]
      },
      "available": true,
      "cloud": "aws",
      "region": "us-east-1",
      "type": "s3"
    }
  ],
  "aliases": [],
  "checksums": [
    {
      "checksum": "73d643ec3f4beb9020eef0beed440ad0",
      "type": "md5"
    }
  ],
  "created_time": null,
  "description": null,
  "form": "object",
  "id": "dg.4503/2ea50456-60f6-4b4c-92cc-5fc6776343ac",
  "index_created_time": "2026-06-12T18:06:22.650954",
  "index_updated_time": "2026-06-12T18:06:22.650965",
  "mime_type": "application/json",
  "name": "test_valid",
  "self_uri": "drs://PREFIX:2ea50456-60f6-4b4c-92cc-5fc6776343ac",
  "size": 9,
  "updated_time": null,
  "version": null
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

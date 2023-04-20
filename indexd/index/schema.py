POST_RECORD_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema",
    "type": "object",
    "additionalProperties": False,
    "description": "Create a new index from hash & size",
    "required": ["size", "hashes", "urls", "form"],
    "properties": {
        "baseid": {
            "type": "string",
            "pattern": "^.*[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$",
        },
        "form": {"enum": ["object", "container", "multipart"]},
        "size": {
            "description": "Size of the data being indexed in bytes",
            "type": "integer",
            "minimum": 0,
        },
        "file_name": {
            "description": "optional file name of the object",
            "type": "string",
        },
        "metadata": {
            "description": "optional metadata of the object",
            "type": "object",
        },
        "urls_metadata": {
            "description": "optional urls metadata of the object",
            "type": "object",
        },
        "version": {
            "description": "optional version string of the object",
            "type": "string",
        },
        "description": {
            "description": "optional description string of the object",
            "type": "string",
        },
        "uploader": {
            "description": "optional uploader of the object",
            "type": "string",
        },
        "urls": {"type": "array", "items": {"type": "string"}},
        "acl": {"type": "array", "items": {"type": "string"}},
        "authz": {
            "description": "optional authorization rules of the object",
            "type": "array",
            "items": {"type": "string"},
        },
        "did": {
            "type": "string",
            "pattern": "^.*[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$",
        },
        "content_created_date": {
            "description": "Timestamp of content creation. Refers to the underyling content, not the JSON object.",
            "type": "string",
            "format": "date-time",
        },
        "content_updated_date": {
            "description": "Timestamp of content update, identical to created_time in systems that do not support updates. Refers to the underyling content, not the JSON object.",
            "type": "string",
            "format": "date-time",
        },
        "hashes": {
            "type": "object",
            "properties": {
                "md5": {"type": "string", "pattern": "^[0-9a-f]{32}$"},
                "sha1": {"type": "string", "pattern": "^[0-9a-f]{40}$"},
                "sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
                "sha512": {"type": "string", "pattern": "^[0-9a-f]{128}$"},
                "crc": {"type": "string", "pattern": "^[0-9a-f]{8}$"},
                "etag": {"type": "string", "pattern": "^[0-9a-f]{32}(-\d+)?$"},
            },
            "anyOf": [
                {"required": ["md5"]},
                {"required": ["sha1"]},
                {"required": ["sha256"]},
                {"required": ["sha512"]},
                {"required": ["crc"]},
                {"required": ["etag"]},
            ],
        },
    },
}

PUT_RECORD_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema",
    "type": "object",
    "additionalProperties": False,
    "description": "Update an index",
    "properties": {
        "urls": {"type": "array", "items": {"type": "string"}},
        "acl": {"type": "array", "items": {"type": "string"}},
        "authz": {"type": "array", "items": {"type": "string"}},
        "file_name": {"type": ["string", "null"]},
        "version": {"type": ["string", "null"]},
        "uploader": {"type": ["string", "null"]},
        "metadata": {"type": "object"},
        "urls_metadata": {"type": "object"},
        "description": {"type": ["string", "null"]},
        "content_created_date": {"type": ["string", "null"], "format": "date-time"},
        "content_updated_date": {"type": ["string", "null"], "format": "date-time"},
    },
}

RECORD_ALIAS_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema",
    "type": "object",
    "additionalProperties": False,
    "description": "Aliases that can be used in place of an Index record's DID",
    "properties": {
        "aliases": {
            "type": "array",
            "items": {"type": "object", "properties": {"value": {"type": "string"}}},
        }
    },
}

BUNDLE_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema",
    "type": "object",
    "additionalProperties": False,
    "description": "Creates a new bundle",
    "required": ["bundles"],
    "properties": {
        "bundle_id": {
            "type": "string",
        },
        "name": {
            "description": "Required bundle name created my author of the bundle",
            "type": "string",
        },
        "bundles": {
            "description": "Expanded bundles and objects.",
            "type": "array",
        },
        "size": {
            "description": "Sum of size of objects inside bundles.",
            "type": "integer",
            "minimum": 0,
        },
        "checksums": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["type", "checksum"],
                "properties": {
                    "type": {
                        "enum": ["md5", "sha1", "sha256", "sha512", "crc", "etag"],
                    },
                    "checksum": {"type": "string"},
                },
                "allOf": [  # TODO: update jsonschema>=3.0.0 to actually use this and remove manual validation
                    {
                        "if": {"properties": {"type": {"const": "md5"}}},
                        "then": {
                            "properties": {"checksum": {"pattern": "^[0-9a-f]{32}$"}}
                        },
                    },
                    {
                        "if": {"properties": {"type": {"const": "sha1"}}},
                        "then": {
                            "properties": {"checksum": {"pattern": "^[0-9a-f]{40}$"}}
                        },
                    },
                    {
                        "if": {"properties": {"type": {"const": "sha256"}}},
                        "then": {
                            "properties": {"checksum": {"pattern": "^[0-9a-f]{64}$"}}
                        },
                    },
                    {
                        "if": {"properties": {"type": {"const": "sha512"}}},
                        "then": {
                            "properties": {"checksum": {"pattern": "^[0-9a-f]{128}$"}}
                        },
                    },
                    {
                        "if": {"properties": {"type": {"const": "crc"}}},
                        "then": {
                            "properties": {"checksum": {"pattern": "^[0-9a-f]{8}$"}}
                        },
                    },
                    {
                        "if": {"properties": {"type": {"const": "etag"}}},
                        "then": {
                            "properties": {
                                "checksum": {"pattern": "^[0-9a-f]{32}(-\d+)?$"}
                            }
                        },
                    },
                ],
            },
        },
        "description": {"type": "string"},
        "version": {
            "description": "optional version string of the object",
            "type": "string",
        },
        "aliases": {
            "description": "Optional",
            "type": "array",
        },
    },
}

UPDATE_ALL_VERSIONS_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema",
    "type": "object",
    "additionalProperties": False,
    "description": "The metadata to update for all versions of the record. Only some fields can be updated in this way.",
    "properties": {
        "acl": {"type": "array", "items": {"type": "string"}},
        "authz": {"type": "array", "items": {"type": "string"}},
    },
}

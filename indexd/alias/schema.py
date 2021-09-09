PUT_RECORD_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema",
    "type": "object",
    "additionalProperties": False,
    "description": "Update or create an alias",
    "required": ["size", "hashes", "release"],
    "properties": {
        "release": {
            "description": "Access control for this data",
            "enum": ["public", "private", "controlled"],
        },
        "size": {
            "description": "Size of the data being indexed in bytes",
            "type": "integer",
            "minimum": 0,
        },
        "keeper_authority": {
            "description": "Who controls the alias pointing to this data?",
            "type": "string",
        },
        "host_authorities": {
            "description": "Who hosts the data?",
            "type": "array",
            "items": {"type": "string"},
        },
        "metastring": {"description": "Further dataset identifiers", "type": "string"},
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

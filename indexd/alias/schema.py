PUT_RECORD_SCHEMA = {
  "$schema": "http://json-schema.org/schema#",
  "type": "object",
  "additionalProperties": False,
  "description": "Update or create an alias",
  "required": [
    "size",
    "hashes",
    "urls",
    "release"
  ],
  "properties": {
    "release": {
      "description": "Access control for this data",
      "enum": [
        "public",
        "private",
        "controlled"
      ]
    },
    "size": {
      "description": "Size of the data being indexed in bytes",
      "type": "integer",
      "minimum": 0
    },
    "urls": {
      "type": "array",
      "items": {
        "type": "string"
      }
    },
    "keeper_authority": {
      "description": "Who controls the alias pointing to this data?",
      "type": "string"
    },
    "host_authority": {
      "description": "Who hosts the data?",
      "type": "array",
      "items": {
        "type": "string"
      }
    },
    "metadata": {
      "description": "Further dataset identifiers",
      "type": "string"
    },
    "hashes": {
      "type": "object",
      "properties": {
        "md5": {
          "type": "string",
          "pattern": "^[0-9a-f]{32}$"
        },
        "sha1": {
          "type": "string",
          "pattern": "^[0-9a-f]{40}$"
        },
        "sha256": {
          "type": "string",
          "pattern": "^[0-9a-f]{64}$"
        },
        "sha512": {
          "type": "string",
          "pattern": "^[0-9a-f]{128}$"
        }
      },
      "oneOf": [
        {
          "required": [
            "md5"
          ]
        },
        {
          "required": [
            "sha1"
          ]
        },
        {
          "required": [
            "sha256"
          ]
        },
        {
          "required": [
            "sha512"
          ]
        }
      ]
    }
  }
}

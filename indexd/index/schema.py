POST_RECORD_SCHEMA = {
  "$schema": "http://json-schema.org/schema#",
  "type": "object",
  "additionalProperties": False,
  "description": "Create a new index from hash & size",
  "required": [
    "size",
    "hashes",
    "urls",
    "form"
  ],
  "properties": {
    "baseid": {
      "type": "string",
      "pattern": "^.*[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$"
    },
    "form": {
      "enum": [
        "object",
        "container",
        "multipart"
      ]
    },
    "size": {
      "description": "Size of the data being indexed in bytes",
      "type": "integer",
      "minimum": 0
    },
    "file_name": {
      "description": "optional file name of the object",
      "type": "string",
    },
    "metadata": {
      "description": "optional metadata of the object",
      "type": "object"
    },
    "version": {
      "description": "optional version string of the object",
      "type": "string",
    },
    "urls": {
      "type": "array",
      "items": {
        "type": "string"
      }
    },
    "did": {
      "type": "string",
      "pattern": "^.*[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$"
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
        },
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

PUT_RECORD_SCHEMA = {
  "$schema": "http://json-schema.org/schema#",
  "type": "object",
  "additionalProperties": False,
  "description": "Update an index",
  "properties": {
    "urls": {
      "type": "array",
      "items": {
        "type": "string"
      }
    },
    "file_name": {
        "type": "string"
    },
    "version": {
        "type": "string"
    },
    "rev": {
      "type": "string",
      "pattern": "^[0-9a-f]{8}$",
     },
  }
}

POST_RECORD_SCHEMA = {
    '$schema': 'http://json-schema.org/schema#',
    'type': 'object',
    'additionalProperties': false,
    'description': 'Create a new index from hash & size',
    'required': ['size', 'hashes', 'urls', 'form'],
    'properties': {
        'form': {
            'enum': ['object', 'container', 'multipart'],
        },
        'size': {
            'description': 'Size of the data being indexed in bytes',
            'type': 'integer',
            'minimum': 0,
        },
        'urls': {
            'type': 'array',
            'items': {'type': 'string'},
        },
        'hashes': {
            'type': 'object',
            'additionalProperties': {
                'type': 'object',
                'properties': {
                    'md5': {
                        'type': 'string',
                         'pattern': '^[0-9a-f]{32}$',
                    },
                    'sha1': {
                        'type': 'string',
                         'pattern': '^[0-9a-f]{40}$',
                    },
                    'sha256': {
                        'type': 'string',
                         'pattern': '^[0-9a-f]{64}$',
                    },
                    'sha512': {
                        'type': 'string',
                         'pattern': '^[0-9a-f]{128}$',
                    },
                },
            },
        },
    },
}

PUT_RECORD_SCHEMA = {
    '$schema': 'http://json-schema.org/schema#',
    'type': 'object',
    'additionalProperties': false,
    'description': 'Update an existing index',
    'required': ['size', 'hashes', 'urls', 'rev'],
    'properties': {
        'rev': {
            'type': 'string',
            'pattern': '^[0-9a-f]{8}$',
        },
        'size': {
            'description': 'Size of the data being indexed in bytes',
            'type': 'integer',
            'minimum': 0,
        },
        'urls': {
            'type': 'array',
            'items': {'type': 'string'},
        },
        'hashes': {
            'type': 'object',
            'additionalProperties': {
                'type': 'object',
                'properties': {
                    'md5': {
                        'type': 'string',
                         'pattern': '^[0-9a-f]{32}$',
                    },
                    'sha1': {
                        'type': 'string',
                         'pattern': '^[0-9a-f]{40}$',
                    },
                    'sha256': {
                        'type': 'string',
                         'pattern': '^[0-9a-f]{64}$',
                    },
                    'sha512': {
                        'type': 'string',
                         'pattern': '^[0-9a-f]{128}$',
                    },
                },
            },
        },
    },
}

POST_RECORD_SCHEMA = {
    '$schema': 'http://json-schema.org/schema#',
    'type': 'object',
    'properties': {
        'form': {
            'enum': ['object', 'container', 'multipart'],
        },
        'size': {
            'type': ['integer', 'null'],
        },
        'urls': {
            'type': 'object',
            'patternProperties': {
                '': {'type': 'string'}
            }
        },
        'hashes': {
            'type': 'object',
            'patternProperties': {
                '': {'type': 'string'},
            },
        },
    },
}

PUT_RECORD_SCHEMA = {
    '$schema': 'http://json-schema.org/schema#',
    'type': 'object',
    'properties': {
        'rev': {
            'type': 'string',
        },
        'size': {
            'type': ['integer', 'null'],
        },
        'urls': {
            'type': 'object',
            'patternProperties': {
                '': {'type': 'string'}
            }
        },
        'hashes': {
            'type': 'object',
            'patternProperties': {
                '': {'type': 'string'},
            },
        },
    },
}

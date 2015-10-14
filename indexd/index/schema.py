POST_RECORD_SCHEMA = {
    '$schema': 'http://json-schema.org/schema#',
    'type': 'object',
    'properties': {
        'hash': {
            'type': 'object',
            'patternProperties': {
                '': {'type': 'string'},
            },
        },
        'urls': {
            'type': 'array',
            'items': {'type': 'string'},
        },
    },
}

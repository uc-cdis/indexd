HYPER = {
    '$schema': 'http://json-schema.org/schema#',
    'type': 'object',
    'description': 'Index service.',
    'mediaType': 'application/json',
    'properties': {
        # TODO
    },
}

POST_RECORD = {
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

PUT_RECORD = {
}

DELETE_RECORD = {
}

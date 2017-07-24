application_config_schema = {
    'type': 'object',
    'properties': {
        'application_command': {'type': 'string'},
        'local_input_files': {
            'type': 'list',
            'items': {'type': 'object'}
        },
        'local_result_files': {
            'type': 'object',
            'patternProperties': {
                '^[a-zA-Z0-9_-]+$': {'type': 'object'}
            },
            'additionalProperties': False
        }
    },
    'required': ['application_command', 'local_input_files', 'local_result_files'],
    'additionalProperties': False
}

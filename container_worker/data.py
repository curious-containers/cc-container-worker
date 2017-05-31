import os
import json
from uuid import uuid4
from inspect import getmembers, isfunction

from container_worker import downloaders, custom_downloaders, uploaders, custom_uploaders

FILE_DIR = os.path.expanduser('~')
FILES_INFO_PATH = os.path.expanduser('~/files.json')


def _get_functions(modules):
    funcs = {}
    for module in modules:
        for name, t in getmembers(module):
            if isfunction(t) and not name.startswith('_'):
                funcs[name] = getattr(module, name)
    return funcs


def dc_download(input_files, input_file_keys):
    connectors = _get_functions([downloaders, custom_downloaders])
    files = {}
    for input_file, input_file_key in zip(input_files, input_file_keys):
        local_input_file = {'dir': FILE_DIR, 'name': str(uuid4())}
        files[input_file_key] = {
            'input_file': input_file,
            'local_input_file': local_input_file
        }
        connector = connectors[input_file['connector_type']]
        connector(input_file['connector_access'], local_input_file)
    with open(FILES_INFO_PATH, 'w') as f:
        json.dump(files, f)


def ac_download(input_files, local_input_files):
    connectors = _get_functions([downloaders, custom_downloaders])
    for input_file, local_input_file in zip(input_files, local_input_files):
        connector = connectors[input_file['connector_type']]
        connector(input_file['connector_access'], local_input_file)


def ac_upload(result_files, local_result_files, meta_data):
    connectors = _get_functions([uploaders, custom_uploaders])
    for result_file in result_files:
        key = result_file['local_result_file']
        local_result_file = local_result_files[key]
        connector = connectors[result_file['connector_type']]
        connector(
            result_file['connector_access'],
            local_result_file,
            meta_data if result_file.get('add_meta_data') else None
        )


def tracing_upload(tracing_file, local_tracing_file, meta_data):
    connectors = _get_functions([uploaders, custom_uploaders])
    connector = connectors[tracing_file['connector_type']]
    connector(
        tracing_file['connector_access'],
        local_tracing_file,
        meta_data if tracing_file.get('add_meta_data') else None
    )

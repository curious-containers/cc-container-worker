import os
import json
from uuid import uuid4
from inspect import getmembers, isfunction

from container_worker import downloaders, custom_downloaders, uploaders, custom_uploaders

FILE_DIR = os.path.expanduser('~')


class File:
    def __init__(self, data, local_file_dir, local_file_name):
        self.connector_type = data['connector_type']
        self.connector_access = data['connector_access']
        self.local_file_dir = local_file_dir
        self.local_file_name = local_file_name

    def exists(self):
        local_file_path = os.path.join(self.local_file_dir, self.local_file_name)
        return os.path.isfile(local_file_path)


def _get_functions(modules):
    funcs = {}
    for module in modules:
        for name, t in getmembers(module):
            if isfunction(t) and not name.startswith('_'):
                funcs[name] = getattr(module, name)
    return funcs


class DCFileManager:
    def __init__(self, input_files, input_file_keys):
        connectors = _get_functions([downloaders, custom_downloaders])
        self._files = {}
        for input_file, input_file_key in zip(input_files, input_file_keys):
            self._files[input_file_key] = File(input_file, FILE_DIR, str(uuid4()))

        # call connectors to download files
        for _, file in self._files.items():
            connector = connectors[file.connector_type]
            connector(file.connector_access, file.local_file_dir, file.local_file_name)

    def get_file(self, input_file_key):
        return self._files[input_file_key]


def ac_download(input_files, local_input_files):
    connectors = _get_functions([downloaders, custom_downloaders])
    files = []
    for input_file, local_input_file in zip(input_files, local_input_files):
        files.append(File(input_file, local_input_file['dir'], local_input_file['name']))

    # call connectors to download files
    for file in files:
        connector = connectors[file.connector_type]
        connector(file.connector_access, file.local_file_dir, file.local_file_name)


def ac_upload(result_files, local_result_files):
    connectors = _get_functions([uploaders, custom_uploaders])
    for result_file, local_result_file in zip(result_files, local_result_files):
        if not result_file:
            continue
        file = File(result_file, local_result_file['dir'], local_result_file['name'])
        if not file.exists():
            if local_result_file.get('optional'):
                continue
            raise Exception('Result file has not been created: {}'.format(
                json.dumps(local_result_file)
            ))
        connector = connectors[file.connector_type]
        connector(file.connector_access, file.local_file_dir, file.local_file_name)

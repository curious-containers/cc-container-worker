import os
import requests
from os.path import expanduser
from paramiko import SSHClient, AutoAddPolicy
from uuid import uuid4
from requests.auth import HTTPBasicAuth, HTTPDigestAuth

from container_worker.helper import key_generator, equal_keys

LOCAL_FILE_BASE_DIR = expanduser('~')


class FileManager:
    def __init__(self, input_files):
        self.file_handlers = []
        self._assign_file_handlers(input_files)

    def _assign_file_handlers(self, input_files):
        for input_file in input_files:
            if 'ssh_host' in input_file:
                self.file_handlers.append(SSHFileHandler(input_file))
            elif 'http_url' in input_file:
                self.file_handlers.append(HTTPFileHandler(input_file))
            else:
                raise Exception('Input file does not contain any key like ssh_host or http_url.')

    def input_file_keys(self):
        return [file_handler.file_key for file_handler in self.file_handlers]

    def find_file_handler(self, json_request):
        for file_handler in self.file_handlers:
            if file_handler.is_request_valid(json_request):
                return file_handler
        raise Exception('Data container does not provide a file matching the specified parameters.')


class SSHFileHandler:
    def __init__(self, input_file):
        self.host = input_file['ssh_host']
        self.username = input_file['ssh_username']
        self.password = input_file['ssh_password']
        self.file_dir = input_file['ssh_file_dir']
        self.file_name = input_file['ssh_file_name']
        self.file_key = key_generator()
        self._local_file_name = str(uuid4())
        self._retrieve()

    def is_request_valid(self, json_request):
        if not json_request['ssh_host'] == self.host:
            return False
        if not json_request['ssh_username'] == self.username:
            return False
        if not json_request['ssh_file_dir'] == self.file_dir:
            return False
        if not json_request['ssh_file_name'] == self.file_name:
            return False
        if not equal_keys(json_request['input_file_key'], self.file_key):
            raise Exception('Value of parameter input_file_key is not valid for requested file.')
        return True

    def local_file_dir(self):
        return LOCAL_FILE_BASE_DIR

    def local_file_name(self):
        return self._local_file_name

    def _retrieve(self):
        local_file_dir = self.local_file_dir()
        local_file_name = self.local_file_name()

        if not os.path.exists(local_file_dir):
            os.makedirs(local_file_dir)

        remote_file_path = os.path.join(self.file_dir, self.file_name)
        local_file_path = os.path.join(local_file_dir, local_file_name)

        with SSHClient() as client:
            client.set_missing_host_key_policy(AutoAddPolicy())
            client.connect(
                self.host,
                username=self.username,
                password=self.password
            )
            with client.open_sftp() as sftp:
                sftp.get(remote_file_path, local_file_path)


class HTTPFileHandler:
    def __init__(self, input_file):
        self.url = input_file['http_url']
        self.data = input_file.get('http_data')
        self.auth = auth(input_file.get('http_auth'))
        self.ssl_verify = input_file.get('http_ssl_verify', True)
        self.file_key = key_generator()
        self._local_file_name = str(uuid4())
        self._retrieve()

    def is_request_valid(self, json_request):
        if not json_request['http_url'] == self.url:
            return False
        if not json_request.get['http_data'] == self.data:
            return False
        if not equal_keys(json_request['input_file_key'], self.file_key):
            raise Exception('Value of parameter input_file_key is not valid for requested file.')
        return True

    def local_file_dir(self):
        return LOCAL_FILE_BASE_DIR

    def local_file_name(self):
        return self._local_file_name

    def _retrieve(self):
        local_file_dir = self.local_file_dir()
        local_file_name = self.local_file_name()

        if not os.path.exists(local_file_dir):
            os.makedirs(local_file_dir)

        r = requests.get(
            self.url,
            json=self.data,
            auth=self.auth,
            verify=self.ssl_verify,
            stream=True
        )

        r.raise_for_status()

        local_file_path = os.path.join(local_file_dir, local_file_name)

        with open(local_file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)

        r.raise_for_status()


def auth(http_auth):
    if not http_auth:
        return None

    if http_auth.get('basic_username'):
        return HTTPBasicAuth(
            http_auth.get('basic_username'),
            http_auth.get('basic_password')
        )

    if http_auth.get('digest_username'):
        return HTTPDigestAuth(
            http_auth.get('digest_username'),
            http_auth.get('digest_password')
        )

    raise Exception('Authorization information is not valid.')

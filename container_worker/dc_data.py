import os
import requests
from os.path import expanduser
from paramiko import SSHClient, AutoAddPolicy
from uuid import uuid4
from requests.auth import HTTPBasicAuth, HTTPDigestAuth
from werkzeug.exceptions import BadRequest

from container_worker.helper import equal_keys

LOCAL_FILE_BASE_DIR = expanduser('~')


class FileManager:
    def __init__(self, input_files, input_file_keys):
        self.file_handlers = []
        self._assign_file_handlers(input_files, input_file_keys)

    def _assign_file_handlers(self, input_files, input_file_keys):
        for input_file, input_file_key in zip(input_files, input_file_keys):
            if 'ssh_host' in input_file:
                self.file_handlers.append(SSHFileHandler(input_file, input_file_key))
            elif 'http_url' in input_file:
                self.file_handlers.append(HTTPFileHandler(input_file, input_file_key))
            else:
                raise Exception('Input file does not contain any key like ssh_host or http_url.')

    def find_file_handler(self, json_request):
        for file_handler in self.file_handlers:
            if file_handler.is_request_valid(json_request):
                return file_handler
        raise BadRequest('Data container does not provide a file matching the specified input_file_key.')


class SSHFileHandler:
    def __init__(self, input_file, input_file_key):
        self.host = input_file['ssh_host']
        self.username = input_file['ssh_username']
        self.password = input_file['ssh_password']
        self.file_dir = input_file['ssh_file_dir']
        self.file_name = input_file['ssh_file_name']
        self.file_key = input_file_key
        self._local_file_name = str(uuid4())
        self._retrieve()

    def is_request_valid(self, json_request):
        if equal_keys(json_request['input_file_key'], self.file_key):
            return True
        return False

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
    def __init__(self, input_file, input_file_key):
        self.url = input_file['http_url']
        self.auth = auth(input_file.get('http_auth'))
        self.ssl_verify = input_file.get('http_ssl_verify', True)
        self.file_key = input_file_key
        self._local_file_name = str(uuid4())
        self._retrieve()

    def is_request_valid(self, json_request):
        if equal_keys(json_request['input_file_key'], self.file_key):
            return True
        return False

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

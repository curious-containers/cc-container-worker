import requests
import os
import json
from paramiko import SSHClient, AutoAddPolicy

from container_worker.dc_data import SSHFileHandler as SSHFH
from container_worker.dc_data import HTTPFileHandler as HTTPFH
from container_worker.dc_data import auth
from container_worker.helper import prepare_response


class FileManager:
    def __init__(self, input_files, local_input_files):
        self.file_handlers = []
        self._assign_file_handlers(input_files, local_input_files)

    def _assign_file_handlers(self, input_files, local_input_files):
        for input_file, local_input_file in zip(input_files, local_input_files):
            if 'ssh_host' in input_file:
                self.file_handlers.append(SSHFileHandler(input_file, local_input_file))
            elif 'http_url' in input_file:
                self.file_handlers.append(HTTPFileHandler(input_file, local_input_file))
            else:
                raise Exception('Input file does not contain any key like ssh_host or http_url.')


class SSHFileHandler(SSHFH):
    def __init__(self, input_file, local_input_file):
        self.local_input_file_dir = local_input_file['dir']
        self.local_input_file_name = local_input_file['name']
        SSHFH.__init__(self, input_file)

    def local_file_dir(self):
        return self.local_input_file_dir

    def local_file_name(self):
        return self.local_input_file_name


class HTTPFileHandler(HTTPFH):
    def __init__(self, input_file, local_input_file):
        self.local_input_file_dir = local_input_file['dir']
        self.local_input_file_name = local_input_file['name']
        HTTPFH.__init__(self, input_file)

    def local_file_dir(self):
        return self.local_input_file_dir

    def local_file_name(self):
        return self.local_input_file_name


def retrieve_files(json_response, config):
    for input_file, local_input_file in zip(json_response, config['main']['local_input_files']):
        if not os.path.exists(local_input_file['dir']):
            os.makedirs(local_input_file['dir'])

        r = requests.get(
            input_file['data_container_url'],
            json=input_file,
            stream=True
        )

        r.raise_for_status()

        local_file_path = os.path.join(local_input_file['dir'], local_input_file['name'])

        with open(local_file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)

        r.raise_for_status()


def send_results(json_input, config):
    for result_file, local_result_file in zip(json_input, config['main']['local_result_files']):
        if not result_file:
            continue
        if 'ssh_host' in result_file:
            _ssh_send_results(result_file, local_result_file)
        elif 'http_url' in result_file:
            _http_send_results(result_file, local_result_file)
        elif 'json_url' in result_file:
            _http_send_results(result_file, local_result_file)
        else:
            raise Exception('Send config for result file not appropriate: {}'.format(
                json.dumps(prepare_response(result_file)))
            )


def _json_send_results(result_file, local_result_file):
    local_result_file_path = os.path.join(local_result_file['dir'], local_result_file['name'])
    with open(local_result_file_path) as f:
        data = json.load(f)

    # set additional json fields specified in result_file
    if result_file.get('json_data'):
        for key, val in result_file['json_data'].items():
            data[key] = val

    r = requests.post(
        result_file['json_url'],
        json=data,
        auth=auth(result_file.get('json_auth'))
    )
    r.raise_for_status()


def _http_send_results(result_file, local_result_file):
    local_result_file_path = os.path.join(local_result_file['dir'], local_result_file['name'])

    files = {
        'file': (result_file.get('http_file_name'), open(local_result_file_path, 'rb'), 'application/octet-stream')
    }
    r = requests.post(
        result_file['http_url'],
        files=files,
        auth=auth(result_file.get('http_auth'))
    )
    r.raise_for_status()


def _ssh_send_results(result_file, local_result_file):
    local_result_file_path = os.path.join(local_result_file['dir'], local_result_file['name'])
    ssh_file_dir = result_file['ssh_file_dir']
    ssh_file_path = os.path.join(ssh_file_dir, result_file['ssh_file_name'])

    with SSHClient() as client:
        client.set_missing_host_key_policy(AutoAddPolicy())
        client.connect(
            result_file['ssh_host'],
            username=result_file['ssh_username'],
            password=result_file['ssh_password']
        )
        with client.open_sftp() as sftp:
            mkdir_p(sftp, ssh_file_dir)
            sftp.put(local_result_file_path, ssh_file_path)


# source http://stackoverflow.com/a/14819803
def mkdir_p(sftp, remote_directory):
    if remote_directory == '/':
        sftp.chdir('/')
        return
    if remote_directory == '':
        return
    try:
        sftp.chdir(remote_directory)
    except IOError:
        dirname, basename = os.path.split(remote_directory.rstrip('/'))
        mkdir_p(sftp, dirname)
        sftp.mkdir(basename)
        sftp.chdir(basename)
        return True
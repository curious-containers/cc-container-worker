import requests
import os
import json as json_module
from paramiko import SSHClient, AutoAddPolicy

from container_worker import helper


def http(connector_access, local_result_file):
    local_file_dir = local_result_file['dir']
    local_file_name = local_result_file['name']
    local_file_path = os.path.join(local_file_dir, local_file_name)

    if not os.path.isfile(local_file_path):
        if local_result_file.get('optional'):
            return
        else:
            raise Exception('Result file does not exist and is not optional: {}'.format(local_file_path))

    http_method = connector_access['method'].lower()
    if http_method == 'put':
        method_func = requests.put
    elif http_method == 'post':
        method_func = requests.post
    else:
        raise Exception('HTTP method not valid: {}'.format(connector_access['method']))

    with open(local_file_path, 'rb') as f:
        r = method_func(
            connector_access['url'],
            data=f,
            auth=helper.auth(connector_access.get('auth')),
            verify=connector_access.get('ssl_verify', True)
        )
        r.raise_for_status()


def json(connector_access, local_result_file):
    local_file_dir = local_result_file['dir']
    local_file_name = local_result_file['name']
    local_file_path = os.path.join(local_file_dir, local_file_name)

    if not os.path.isfile(local_file_path):
        if local_result_file.get('optional'):
            return
        else:
            raise Exception('Result file does not exist and is not optional: {}'.format(local_file_path))

    with open(local_file_path) as f:
        data = json_module.load(f)

    r = requests.post(
        connector_access['url'],
        json=data,
        auth=helper.auth(connector_access.get('auth')),
        verify=connector_access.get('ssl_verify', True)
    )
    r.raise_for_status()


def ssh(connector_access, local_result_file):
    local_file_dir = local_result_file['dir']
    local_file_name = local_result_file['name']
    local_file_path = os.path.join(local_file_dir, local_file_name)

    if not os.path.isfile(local_file_path):
        if local_result_file.get('optional'):
            return
        else:
            raise Exception('Result file does not exist and is not optional: {}'.format(local_file_path))

    with SSHClient() as client:
        client.set_missing_host_key_policy(AutoAddPolicy())
        client.connect(
            connector_access['host'],
            username=connector_access['username'],
            password=connector_access['password']
        )
        with client.open_sftp() as sftp:
            _ssh_mkdir(sftp, connector_access['file_dir'])
            sftp.put(
                os.path.join(local_file_dir, local_file_name),
                os.path.join(connector_access['file_dir'], connector_access['file_name'])
            )


def _ssh_mkdir(sftp, remote_directory):
    # source http://stackoverflow.com/a/14819803
    if remote_directory == '/':
        sftp.chdir('/')
        return
    if remote_directory == '':
        return
    try:
        sftp.chdir(remote_directory)
    except IOError:
        dirname, basename = os.path.split(remote_directory.rstrip('/'))
        _ssh_mkdir(sftp, dirname)
        sftp.mkdir(basename)
        sftp.chdir(basename)
        return True

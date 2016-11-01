import os
import requests
from paramiko import SSHClient, AutoAddPolicy

from container_worker import helper


def http(connector_access, local_file_dir, local_file_name):
    if not os.path.exists(local_file_dir):
        os.makedirs(local_file_dir)

    r = requests.get(
        connector_access['url'],
        auth=helper.auth(connector_access.get('http_auth')),
        verify=connector_access.get('ssl_verify', True),
        stream=True
    )
    r.raise_for_status()

    local_file_path = os.path.join(local_file_dir, local_file_name)

    with open(local_file_path, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)


def ssh(connector_access, local_file_dir, local_file_name):
    if not os.path.exists(local_file_dir):
        os.makedirs(local_file_dir)

    remote_file_path = os.path.join(connector_access['file_dir'], connector_access['file_name'])
    local_file_path = os.path.join(local_file_dir, local_file_name)

    with SSHClient() as client:
        client.set_missing_host_key_policy(AutoAddPolicy())
        client.connect(
            connector_access['host'],
            username=connector_access['username'],
            password=connector_access['password']
        )
        with client.open_sftp() as sftp:
            sftp.get(remote_file_path, local_file_path)

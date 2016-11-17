import requests
import os
import json
import pymongo
import gridfs
import paramiko
import uuid
from bson.objectid import ObjectId

from container_worker import helper


@helper.skip_optional
def http(connector_access, local_result_file, meta_data):
    local_file_path = os.path.join(local_result_file['dir'], local_result_file['name'])

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


@helper.skip_optional
def http_json(connector_access, local_result_file, meta_data):
    local_file_path = os.path.join(local_result_file['dir'], local_result_file['name'])

    with open(local_file_path) as f:
        data = json.load(f)

    if meta_data:
        for key, val in meta_data.items():
            data[key] = val

    r = requests.post(
        connector_access['url'],
        json=data,
        auth=helper.auth(connector_access.get('auth')),
        verify=connector_access.get('ssl_verify', True)
    )
    r.raise_for_status()


@helper.skip_optional
def mongodb_json(connector_access, local_result_file, meta_data):
    local_file_path = os.path.join(local_result_file['dir'], local_result_file['name'])

    with open(local_file_path) as f:
        data = json.load(f)

    if meta_data:
        for key, val in meta_data.items():
            try:
                data[key] = [ObjectId(val)]
            except:
                data[key] = val

    client = _mongodb_client(connector_access)
    db = client[connector_access['dbname']]
    db[connector_access['collection']].insert_one(data)
    client.close()


@helper.skip_optional
def mongodb_gridfs(connector_access, local_result_file, meta_data):
    local_file_path = os.path.join(local_result_file['dir'], local_result_file['name'])

    client = _mongodb_client(connector_access)
    db = client[connector_access['dbname']]
    fs = gridfs.GridFSBucket(db)

    md = connector_access.get('metadata')
    if meta_data:
        if not md:
            md = {}
        for key, val in meta_data.items():
            try:
                md[key] = [ObjectId(val)]
            except:
                md[key] = val

    with open(local_file_path, 'rb') as f:
        fs.upload_from_stream(
            connector_access.get('file_name', str(uuid.uuid4())),
            f,
            chunk_size_bytes=4096,
            metadata=md
        )
    client.close()


def _mongodb_client(connector_access):
    url_parameters = ''
    enable_ssl = connector_access.get('enable_ssl', True)
    ssl_verify = connector_access.get('ssl_verify', True)
    ssl_ca_cert_path = connector_access.get('ssl_ca_cert_path')
    if enable_ssl:
        url_parameters = '?ssl=true'
        if ssl_ca_cert_path:
            url_parameters = '{}&ssl_ca_certs={}'.format(url_parameters, ssl_ca_cert_path)
        elif not ssl_verify:
            url_parameters = '{}&ssl_cert_reqs=CERT_NONE'.format(url_parameters)

    return pymongo.MongoClient('mongodb://{}:{}@{}:{}/{}{}'.format(
        connector_access['username'],
        connector_access['password'],
        connector_access['host'],
        connector_access.get('port', 27017),
        connector_access['db'],
        url_parameters
    ))


@helper.skip_optional
def ssh(connector_access, local_result_file, meta_data):
    local_file_path = os.path.join(local_result_file['dir'], local_result_file['name'])

    with paramiko.SSHClient() as client:
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            connector_access['host'],
            port=connector_access.get('port', 22),
            username=connector_access['username'],
            password=connector_access['password']
        )
        with client.open_sftp() as sftp:
            _ssh_mkdir(sftp, connector_access['file_dir'])
            sftp.put(
                local_file_path,
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

import os
from requests.auth import HTTPBasicAuth, HTTPDigestAuth


def auth(http_auth):
    if not http_auth:
        return None

    if http_auth['auth_type'] == 'basic':
        return HTTPBasicAuth(
            http_auth['username'],
            http_auth['password']
        )

    if http_auth['auth_type'] == 'digest':
        return HTTPDigestAuth(
            http_auth['username'],
            http_auth['password']
        )

    raise Exception('Authorization information is not valid.')


def skip_optional(func):
    """function decorator"""
    def wrapper(connector_access, local_result_file, metadata):
        local_file_path = os.path.join(local_result_file['dir'], local_result_file['name'])
        if not os.path.isfile(local_file_path):
            if local_result_file.get('optional'):
                return
            else:
                raise Exception('Result file does not exist and is not optional: {}'.format(local_file_path))
        return func(connector_access, local_result_file, metadata)
    return wrapper

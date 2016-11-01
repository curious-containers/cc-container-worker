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

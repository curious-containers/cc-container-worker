from os import urandom
from binascii import hexlify
from streql import equals

UNSAFE_KEYS = [
    'password',
    'callback_key',
    'ssh_password',
    'http_data',
    'http_auth',
    'json_data',
    'json_auth'
]


def key_generator():
    return hexlify(urandom(24)).decode('utf-8')


def equal_keys(a, b):
    return equals(a.encode('utf-8'), b.encode('utf-8'))


def prepare_response(data):
    if isinstance(data, dict):
        result = {}
        for key, val in data.items():
            if key in UNSAFE_KEYS:
                val = 10*'*'
            else:
                val = prepare_response(val)
            result[key] = val
        return result
    elif isinstance(data, list):
        return [prepare_response(e) for e in data]
    return data

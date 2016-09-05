from os import urandom
from binascii import hexlify
from streql import equals


def key_generator():
    return hexlify(urandom(24)).decode('utf-8')


def equal_keys(a, b):
    return equals(a.encode('utf-8'), b.encode('utf-8'))

from os import urandom
from binascii import hexlify


def key_generator():
    return hexlify(urandom(24)).decode('utf-8')

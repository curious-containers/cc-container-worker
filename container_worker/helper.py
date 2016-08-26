import random


def _char_range(first, last):
    return [chr(c) for c in range(ord(first), ord(last)+1)]

KEY_LENGTH = 20
KEY_CHARS = _char_range('a', 'z') + _char_range('A', 'Z') + _char_range('0', '9')


def key_generator():
    l = len(KEY_CHARS)
    return ''.join([KEY_CHARS[random.randrange(l)] for _ in range(KEY_LENGTH)])

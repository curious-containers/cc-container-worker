from streql import equals


def equal_keys(a, b):
    return equals(a.encode('utf-8'), b.encode('utf-8'))


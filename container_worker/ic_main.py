import json
from psutil import virtual_memory


def main():
    ram = virtual_memory()
    node = {
        'total_ram': ram.total // (1024 * 1024),
        'reserved_ram': (ram.total - ram.available) // (1024 * 1024),
        'total_cpus': None,
        'reserved_cpus': None
    }
    print(json.dumps(node))

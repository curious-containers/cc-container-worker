import json
from psutil import virtual_memory


def main():
    ram = virtual_memory()
    node = {
        'name': 'local',
        'total_ram': ram.total,
        'reserved_ram': ram.total - ram.available,
        'total_cpus': None,
        'reserved_cpus': None
    }
    print(json.dumps(node))

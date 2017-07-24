from psutil import Process
from os.path import getsize, join, isfile
from time import sleep
from threading import Lock
from math import ceil
from time import time


class Telemetry:
    def __init__(self, process, config):
        self.telemetry_interval_seconds = 5
        self.process = process
        self.config = config
        self.max_vms_memory = 0
        self.max_rss_memory = 0
        self.lock = Lock()
        self.timestamp = time()

    def monitor(self):
        # source: http://stackoverflow.com/a/13607392
        while True:
            try:
                pp = Process(self.process.pid)
                processes = list(pp.children(recursive=True))
                processes.append(pp)

                vms_memory = 0
                rss_memory = 0

                for p in processes:
                    try:
                        mem_info = p.memory_info()
                        rss_memory += mem_info[0]
                        vms_memory += mem_info[1]
                    except:
                        pass
                with self.lock:
                    self.max_vms_memory = max(self.max_vms_memory, vms_memory)
                    self.max_rss_memory = max(self.max_rss_memory, rss_memory)
            except:
                break
            sleep(self.telemetry_interval_seconds)

    def _input_file_sizes(self):
        return [_file_size(f) for f in self.config['local_input_files']]

    def _result_file_sizes(self):
        return {key: _file_size(f) for key, f in self.config['local_result_files'].items()}

    def result(self):
        with self.lock:
            return {
                'max_vms_memory': ceil(self.max_vms_memory / (1024 * 1024)),
                'max_rss_memory': ceil(self.max_rss_memory / (1024 * 1024)),
                'input_file_sizes': self._input_file_sizes(),
                'result_file_sizes': self._result_file_sizes(),
                'wall_time': time() - self.timestamp
            }


def _file_size(f):
    result = None
    try:
        file_path = join(f['dir'], f['name'])
        if isfile(file_path):
            result = {
                'local_file_path': file_path,
                'file_size': ceil(getsize(file_path) / (1024 * 1024))
            }
    except:
        pass
    return result

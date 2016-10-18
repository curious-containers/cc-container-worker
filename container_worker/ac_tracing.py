from threading import Lock
from process_tracing.tracing import Tracing as T
from process_tracing.recording import FileAccessRecord, SyscallRecord


class Tracing:
    def __init__(self, process, config):
        self.process = process
        self.config = config
        self.lock = Lock()

        # Create tracer
        self.tracer = T(self.process, stop=True)

    def start(self):
        # Configure the tracing instance
        self.tracer.runtime_tracing = self.config.get('enabled', False)
        file_access_tracing = self.config.get('file_access', 'none')
        if file_access_tracing != 'none':
            self.tracer.file_access_tracing = (file_access_tracing == 'short')
            self.tracer.file_access_detailed_tracing = (file_access_tracing == 'full')
        else:
            self.tracer.file_access_tracing = False

        syscall_tracing = self.config.get('syscall', 'none')
        if syscall_tracing != 'none':
            self.tracer.syscall_tracing = (syscall_tracing == 'short')
            self.tracer.syscall_argument_tracing = (syscall_tracing == 'full')
        else:
            self.tracer.set_file_access_tracing_enabled = False

        # Start the tracing process (if tracing is requested)
        if self.tracer.runtime_tracing or self.tracer.file_access_tracing or self.tracer.syscall_tracing:
            self.tracer.start()

    def finish(self):
        if self.tracer.is_running():
            self.tracer.wait()

    def _result_processes(self):
        # Collect all process data
        if not self.config.get('enabled', False):
            return None

        result = []
        tracing_records = self.tracer.get_logs()
        for pid in tracing_records.keys():
            p = tracing_records[pid]
            result_stats = p.get_result_stats()
            end_time, exit_code, signal = (None, None, None)
            if result_stats:
                end_time, exit_code, signal = result_stats

            record = {
                'pid': pid,
                'start': p.get_start_time(),
                'end': end_time,
                'exit_code': exit_code,
                'signal': signal
            }
            result.append(record)

        return result

    def _result_file_access(self):
        # Collect file data
        file_access_tracing = self.config.get('file_access', 'none')
        if file_access_tracing == 'none':
            return None
        elif file_access_tracing == 'short':
            return self._result_file_access_short()
        else:
            return self._result_file_access_full()

    def _result_file_access_short(self):
        result = set()
        tracing_records = self.tracer.get_logs()

        for pid in tracing_records.keys():
            p = tracing_records[pid]
            for entry in p.get_log():
                if type(entry) == FileAccessRecord:
                    result.add(entry.filename)

        return list(result)

    def _result_file_access_full(self):
        result = []
        tracing_records = self.tracer.get_logs()
        for pid in tracing_records.keys():
            p = tracing_records[pid]
            for entry in p.get_log():
                if type(entry) == FileAccessRecord:
                    record = {
                        'pid': pid,
                        'filename': entry.filename,
                        'is_directory': entry.is_dir,
                        'exists': entry.exists,
                        'syscall': entry.name,
                        'access_time': entry.timestamp,
                        'syscall_result': entry.result
                    }
                    result.append(record)

        return result

    def _result_syscalls(self):
        # Collect syscall data
        syscall_tracing = self.config.get('syscall', 'none')
        if syscall_tracing == 'none':
            return None
        elif syscall_tracing == 'short':
            return self._result_syscalls_records(include_arguments=False)
        else:
            return self._result_syscalls_records(include_arguments=True)

    def _result_syscalls_records(self, include_arguments):
        result = []
        tracing_records = self.tracer.get_logs()
        for pid in tracing_records.keys():
            p = tracing_records[pid]
            for entry in p.get_log():
                if type(entry) == SyscallRecord:
                    record = {
                        'pid': pid,
                        'name': entry.filename,
                        'start': entry.t_start,
                        'end': entry.t_end,
                        'result': entry.result
                    }

                    if include_arguments:
                        record['attributes'] = []
                        for argument in entry.arguments:
                            argument_record = {
                                'name': argument.name,
                                'type': argument.type,
                                'value': argument.value,
                                'text': argument.text
                            }
                            record['attributes'].append(argument_record)

                    result.append(record)

        return result

    def result(self):
        with self.lock:
            # Wait for the tracer to terminate
            self.tracer.wait()

            processes = self._result_processes()
            file_access = self._result_file_access()
            syscalls = self._result_syscalls()
            result = {}

            if processes:
                result['processes'] = processes

            if file_access:
                result['file_access'] = file_access

            if syscalls:
                result['syscalls'] = syscalls

            return result

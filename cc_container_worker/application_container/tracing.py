import signal
import posix
from process_tracing.tracing import Tracing as T
from process_tracing.constants import TRACING_RECORD_MODE_FILE


class Tracing:
    def __init__(self, process, outfile, config=None):
        # Create tracer
        self.tracer = T(process, stop=True, recording_mode=TRACING_RECORD_MODE_FILE, log_filename=outfile)

        self.process = process
        self.config = config

    def start(self):
        # Configure the tracing instance
        if not self.config:
            self.tracer.runtime_tracing = False
            self.tracer.file_access_tracing = False
            self.tracer.syscall_tracing = False
        else:
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
        else:
            posix.kill(self.process, signal.SIGCONT)

    def finish(self):
        if self.tracer.is_running():
            self.tracer.wait()
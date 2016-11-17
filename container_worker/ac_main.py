import os
import json
import toml
import posix
import signal
from subprocess import Popen, PIPE
from traceback import format_exc
from threading import Thread

from container_worker.data import ac_download, ac_upload
from container_worker.ac_telemetry import Telemetry
from container_worker.ac_tracing import Tracing
from container_worker.ac_sandbox import Sandbox
from container_worker.callbacks import CallbackHandler, DebugCallbackHandler

CONFIG_FILE_PATH = '/opt/config.toml'

LOCAL_TRACING_FILE = {
    'dir': '/var/tmp/cc-tracing',
    'name': 'data.json'
}


def main(settings, debug=False):
    if debug:
        callback_handler = DebugCallbackHandler(settings)
    else:
        callback_handler = CallbackHandler(settings)

    try:
        with open(CONFIG_FILE_PATH) as f:
            config = toml.load(f)
            if debug:
                callback_handler.config = config
        local_result_files = config['main']['local_result_files']
        local_input_files = config['main']['local_input_files']
        application_command = config['main']['application_command']
        assert type(local_result_files) is list
        assert type(local_input_files) is list
        assert type(application_command) is str
    except:
        description = 'Could not load TOML config file from path {}'.format(CONFIG_FILE_PATH)
        callback_handler.send_callback(
            callback_type='started', state='failed', description=description, exception=format_exc()
        )
        exit(3)

    result_files = settings['result_files']

    if len(result_files) != len(local_result_files):
        description = 'Number of local_result_files in config does not match result_files.'
        callback_handler.send_callback(
            callback_type='started', state='failed', description=description
        )
        exit(4)

    for local_result_file in local_result_files:
        try:
            if not os.path.exists(local_result_file['dir']):
                os.makedirs(local_result_file['dir'])
        except:
            pass

    description = 'Container started.'
    response = callback_handler.send_callback(callback_type='started', state='success', description=description)

    input_files = response['input_files']

    if len(input_files) != len(local_input_files):
        description = 'Number of local_input_files in config does not match input_files.'
        callback_handler.send_callback(callback_type='files_retrieved', state='failed', description=description)
        exit(5)

    try:
        ac_download(input_files, local_input_files)
    except:
        description = 'Could not retrieve input files.'
        callback_handler.send_callback(
            callback_type='files_retrieved', state='failed', description=description, exception=format_exc()
        )
        exit(6)

    description = 'Input files retrieved.'
    callback_handler.send_callback(callback_type='files_retrieved', state='success', description=description)

    telemetry_data = None
    tracing_data = None
    try:
        if settings.get('parameters'):
            if isinstance(settings['parameters'], dict):
                application_command = '{} \'{}\''.format(application_command, json.dumps(settings['parameters']))
            elif isinstance(settings['parameters'], list):
                application_command += ''.join([' {}'.format(val) for val in settings['parameters']])
            else:
                raise Exception('Type of parameters not valid: {}'.format(type(settings['parameters'])))

        sandbox = Sandbox(config=settings.get('sandbox'))

        print(application_command)
        sp = Popen(application_command, stdout=PIPE, stderr=PIPE, shell=True, preexec_fn=process_preexec(sandbox))

        tracing = Tracing(sp.pid, config=settings.get('tracing'))
        tracing.start()

        telemetry = Telemetry(sp, config=config)
        t = Thread(target=telemetry.monitor)
        t.start()

        std_out, std_err = sp.communicate()
        tracing.finish()
        return_code = sp.returncode

        # Collect telemetry data
        telemetry_data = telemetry.result()
        if std_out:
            telemetry_data['std_out'] = str(std_out)
        if std_err:
            telemetry_data['std_err'] = str(std_err)
        telemetry_data['return_code'] = return_code

        # Collect tracing data
        tracing_data = tracing.result()
    except:
        callback_handler.send_callback(
            callback_type='processed', state='failed', description='Processing failed.', exception=format_exc()
        )
        exit(8)

    description = 'Processing succeeded.'
    state = 'success'
    exception = None

    if return_code != 0:
        description = 'Processing failed.'
        state = 'failed'

    try:
        if tracing_data:
            tracing_file = settings['tracing'].get('tracing_file')
            if tracing_file:
                if not os.path.exists(LOCAL_TRACING_FILE['dir']):
                    os.makedirs(LOCAL_TRACING_FILE['dir'])
                local_tracing_file_path = os.path.join(LOCAL_TRACING_FILE['dir'], LOCAL_TRACING_FILE['name'])
                with open(local_tracing_file_path, 'w') as f:
                    json.dump(tracing_data, f)
            meta_data = {'application_container_id': settings['container_id']}
            ac_upload(
                [tracing_file],
                [LOCAL_TRACING_FILE],
                meta_data
            )
    except:
        if return_code != 0:
            description = 'Processing failed and tracing file upload failed.'
        else:
            description = 'Tracing file upload failed.'
        state = 'failed'
        exception = format_exc()

    callback_handler.send_callback(
        callback_type='processed',
        state=state,
        description=description,
        exception=exception,
        telemetry=telemetry_data,
    )

    if return_code != 0:
        exit(9)

    try:
        additional_metadata = {'application_container_id': settings['container_id']}
        ac_upload(result_files, local_result_files, additional_metadata)
    except:
        description = 'Could not send result files.'
        callback_handler.send_callback(
            callback_type='results_sent', state='failed', description=description, exception=format_exc()
        )
        exit(10)

    callback_handler.send_callback(
        callback_type='results_sent', state='success', description='Result files sent.'
    )


def process_preexec(sandbox):
    def runner():
        # Halt the process immediatly
        #posix.kill(os.getpid(), signal.SIGSTOP)

        sandbox.enter()

    return runner

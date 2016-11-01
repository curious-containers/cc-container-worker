import os
import json
import toml
from subprocess import Popen, PIPE
from traceback import format_exc
from threading import Thread

from container_worker.data import ac_download_files, ac_upload_results
from container_worker.ac_telemetry import Telemetry
from container_worker.ac_tracing import Tracing
from container_worker.ac_sandbox import Sandbox
from container_worker.callbacks import CallbackHandler, DebugCallbackHandler

CONFIG_FILE_PATH = '/opt/config.toml'


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
    except:
        description = 'Could not load TOML config file from path {}'.format(CONFIG_FILE_PATH)
        callback_handler.send_callback(
            callback_type='started', state='failed', description=description, exception=format_exc()
        )
        exit(3)

    result_files = settings['result_files']

    if len(result_files) != len(config['main']['local_result_files']):
        description = 'Number of local_result_files in config does not match result_files.'
        callback_handler.send_callback(
            callback_type='started', state='failed', description=description
        )
        exit(4)

    try:
        for local_result_file in config['main']['local_result_files']:
            if not os.path.exists(local_result_file['dir']):
                os.makedirs(local_result_file['dir'])
    except:
        description = 'Could not create directories for result files.'
        callback_handler.send_callback(
            callback_type='started', state='failed', description=description, exception=format_exc()
        )
        exit(5)

    description = 'Container started.'
    response = callback_handler.send_callback(callback_type='started', state='success', description=description)

    input_files = response['input_files']

    if len(input_files) != len(config['main']['local_input_files']):
        description = 'Number of local_input_files in config does not match input_files.'
        callback_handler.send_callback(callback_type='files_retrieved', state='failed', description=description)
        exit(6)

    try:
        ac_download_files(input_files, config['main']['local_input_files'])
    except:
        description = 'Could not retrieve input files.'
        callback_handler.send_callback(
            callback_type='files_retrieved', state='failed', description=description, exception=format_exc()
        )
        exit(7)

    description = 'Input files retrieved.'
    callback_handler.send_callback(callback_type='files_retrieved', state='success', description=description)

    telemetry_data = None
    try:
        command = config['main']['application_command']

        if settings.get('parameters'):
            if isinstance(settings['parameters'], dict):
                command = '{} \'{}\''.format(command, json.dumps(settings['parameters']))
            elif isinstance(settings['parameters'], list):
                command += ''.join([' {}'.format(val) for val in settings['parameters']])
            else:
                raise Exception('Type of parameters not valid: {}'.format(type(settings['parameters'])))

        sandbox = Sandbox(config=settings.get('sandbox'))

        print(command)
        sp = Popen(command, stdout=PIPE, stderr=PIPE, shell=True, preexec_fn=sandbox.enter)

        tracing = Tracing(sp.pid, config=settings.get('tracing'))
        tracing.start()

        telemetry = Telemetry(sp, config=config)
        t = Thread(target=telemetry.monitor)
        t.start()

        std_out, std_err = sp.communicate()
        tracing.finish()
        return_code = sp.returncode

        # Collect telemetry
        telemetry_data = telemetry.result()
        tracing_data = tracing.result()
        if tracing_data:
            telemetry_data['tracing'] = tracing_data
    except:
        description = 'Processing of application command failed.'
        callback_handler.send_callback(
            callback_type='processed', state='failed', description=description, exception=format_exc()
        )
        exit(8)

    if return_code != 0:
        description = 'Processing of application command returns error code {}.'.format(return_code)
        callback_handler.send_callback(
            callback_type='processed', state='failed', description=description, exception=str(std_err)
        )
        exit(9)

    description = 'Processing succeeded.'
    callback_handler.send_callback(
        callback_type='processed',
        state='success',
        description=description,
        telemetry=telemetry_data,
    )

    try:
        ac_upload_results(result_files, config['main']['local_result_files'])
    except:
        description = 'Could not send result files.'
        callback_handler.send_callback(
            callback_type='results_sent', state='failed', description=description,
            exception=format_exc()
        )
        exit(10)

    description = 'Result files sent.'
    callback_handler.send_callback(
        callback_type='results_sent', state='success', description=description
    )

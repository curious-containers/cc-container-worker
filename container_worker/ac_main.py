import os
import json
from subprocess import Popen, PIPE
from traceback import format_exc
from threading import Thread

from container_worker.data import ac_download, ac_upload, tracing_upload
from container_worker.ac_telemetry import Telemetry
from container_worker.ac_tracing import Tracing
from container_worker.ac_sandbox import Sandbox
from container_worker.callbacks import CallbackHandler

CONFIG_FILE_PATH = '/opt/config.json'

LOCAL_TRACING_FILE = {
    'dir': '/var/tmp/cc-tracing',
    'name': 'data.csv',
    'optional': True
}


def main(settings):
    callback_handler = CallbackHandler(settings)

    try:
        with open(CONFIG_FILE_PATH) as f:
            config = json.load(f)
        local_result_files = config['local_result_files']
        local_input_files = config['local_input_files']
        application_command = config['application_command']
        assert type(local_result_files) is dict
        assert type(local_input_files) is list
        assert type(application_command) is str
    except:
        description = 'Could not load JSON config file from path {}'.format(CONFIG_FILE_PATH)
        callback_handler.send_callback(
            callback_type='started', state='failed', description=description, exception=format_exc()
        )
        exit(3)

    for key, val in local_result_files.items():
        try:
            if not os.path.exists(val['dir']):
                os.makedirs(val['dir'])
        except:
            pass

    description = 'Container started.'
    additional_settings = callback_handler.send_callback(callback_type='started', state='success', description=description)

    meta_data = {
        'application_container_id': settings['container_id'],
        'task_id': additional_settings['task_id']
    }

    input_files = additional_settings['input_files']
    result_files = additional_settings['result_files']

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
    try:
        if additional_settings.get('parameters'):
            if isinstance(additional_settings['parameters'], dict):
                application_command = '{} \'{}\''.format(
                    application_command,
                    json.dumps(additional_settings['parameters'])
                )
            elif isinstance(additional_settings['parameters'], list):
                application_command += ''.join([' {}'.format(val) for val in additional_settings['parameters']])
            else:
                raise Exception('Type of parameters not valid: {}'.format(type(additional_settings['parameters'])))

        sandbox = Sandbox(config=additional_settings.get('sandbox'))

        if not os.path.exists(LOCAL_TRACING_FILE['dir']):
            os.makedirs(LOCAL_TRACING_FILE['dir'])

        local_tracing_file_path = os.path.join(LOCAL_TRACING_FILE['dir'], LOCAL_TRACING_FILE['name'])

        print(application_command)
        sp = Popen(application_command, stdout=PIPE, stderr=PIPE, shell=True, preexec_fn=sandbox.enter)

        tracing = Tracing(sp.pid, config=additional_settings.get('tracing'), outfile=local_tracing_file_path)
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
        if additional_settings.get('tracing'):
            tracing_file = additional_settings['tracing'].get('tracing_file')
            if tracing_file:
                tracing_upload(tracing_file, LOCAL_TRACING_FILE, meta_data)
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
        ac_upload(result_files, local_result_files, meta_data)
    except:
        description = 'Could not send result files.'
        callback_handler.send_callback(
            callback_type='results_sent', state='failed', description=description, exception=format_exc()
        )
        exit(10)

    callback_handler.send_callback(
        callback_type='results_sent', state='success', description='Result files sent.'
    )

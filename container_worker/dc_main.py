from subprocess import Popen, PIPE
from flask import Flask, send_from_directory, request
from traceback import format_exc

from container_worker.callbacks import CallbackHandler
from container_worker.dc_data import FileManager


def main(settings, debug=False):
    app = Flask('sftp-data-container')
    callback_handler = CallbackHandler(settings)
    file_manager = None

    if settings.get('mtu'):
        for key, val in settings['mtu'].items():
            command = 'ifconfig {} mtu {}'.format(key, val)
            sp = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
            std_out, std_err = sp.communicate()
            return_code = sp.returncode
            if return_code != 0:
                print('Cloud not set mtu {} for interface {}'.format(val, key))
                exit(2)

    description = 'Container started.'
    callback_handler.send_callback(callback_type='started', state='success', description=description)

    try:
        file_manager = FileManager(settings['input_files'])
    except:
        description = 'Could not retrieve input files.'
        callback_handler.send_callback(
            callback_type='files_retrieved', state='failed', description=description, exception=format_exc()
        )
        exit(3)

    callback_handler.callback['content']['input_file_keys'] = file_manager.input_file_keys()
    description = 'Input files available.'
    callback_handler.send_callback(callback_type='files_retrieved', state='success', description=description)

    @app.route('/', methods=['GET'])
    def files():
        json_request = request.get_json()
        file_handler = file_manager.find_file_handler(json_request)

        return send_from_directory(file_handler.local_file_dir(), file_handler.local_file_name(), as_attachment=True)

    app.run(host='0.0.0.0', port=80)

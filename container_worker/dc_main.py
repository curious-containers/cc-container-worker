from flask import Flask, send_from_directory, request
from traceback import format_exc

from container_worker.callbacks import CallbackHandler
from container_worker.dc_data import FileManager


def main(settings):
    app = Flask('sftp-data-container')
    callback_handler = CallbackHandler(settings)

    description = 'Container started.'
    callback_handler.send_callback(callback_type='started', state='success', description=description)

    if len(settings['input_files']) != len(settings['input_file_keys']):
        description = 'Number of input_file_keys does not match number of input_files.'
        callback_handler.send_callback(
            callback_type='files_retrieved', state='failed', description=description
        )
        exit(2)

    try:
        file_manager = FileManager(settings['input_files'], settings['input_file_keys'])
    except:
        description = 'Could not retrieve input files.'
        callback_handler.send_callback(
            callback_type='files_retrieved', state='failed', description=description, exception=format_exc()
        )
        exit(3)

    description = 'Input files available.'
    callback_handler.send_callback(callback_type='files_retrieved', state='success', description=description)

    @app.route('/', methods=['GET'])
    def files():
        json_request = request.get_json()
        file_handler = file_manager.find_file_handler(json_request)

        return send_from_directory(file_handler.local_file_dir(), file_handler.local_file_name(), as_attachment=True)

    app.run(host='0.0.0.0', port=80)

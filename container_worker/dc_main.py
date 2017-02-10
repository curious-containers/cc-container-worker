from flask import Flask, send_from_directory
from traceback import format_exc

from container_worker.callbacks import CallbackHandler
from container_worker.data import DCFileManager


def main(settings):
    app = Flask('sftp-data-container')
    callback_handler = CallbackHandler(settings)

    description = 'Container started.'
    additional_settings = callback_handler.send_callback(callback_type='started', state='success', description=description)

    if len(additional_settings['input_files']) != len(additional_settings['input_file_keys']):
        description = 'Number of input_file_keys does not match number of input_files.'
        callback_handler.send_callback(
            callback_type='files_retrieved', state='failed', description=description
        )
        exit(2)

    try:
        file_manager = DCFileManager(additional_settings['input_files'], additional_settings['input_file_keys'])
    except:
        description = 'Could not retrieve input files.'
        callback_handler.send_callback(
            callback_type='files_retrieved', state='failed', description=description, exception=format_exc()
        )
        exit(3)

    description = 'Input files available.'
    callback_handler.send_callback(callback_type='files_retrieved', state='success', description=description)

    @app.route('/<key>', methods=['GET'])
    def root(key):
        file = file_manager.get_file(key)
        return send_from_directory(
            file['local_input_file']['dir'],
            file['local_input_file']['name'],
            as_attachment=True
        )

    app.run(host='0.0.0.0', port=80)

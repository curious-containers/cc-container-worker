import json

from flask import Flask, send_from_directory

from cc_container_worker.commons.data import FILES_INFO_PATH

application = Flask('data-container')


with open(FILES_INFO_PATH) as f:
    files = json.load(f)


@application.route('/<key>', methods=['GET'])
def root(key):
    file = files[key]
    return send_from_directory(
        file['local_input_file']['dir'],
        file['local_input_file']['name'],
        as_attachment=True
    )

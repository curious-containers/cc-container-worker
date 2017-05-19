from gunicorn.app.base import BaseApplication
from gunicorn import util
from traceback import format_exc
from multiprocessing import cpu_count

from container_worker.callbacks import CallbackHandler
from container_worker.data import dc_download


class WebApp(BaseApplication):
    def __init__(self, options=None):
        self.options = options or {}
        super(WebApp, self).__init__()

    def load_config(self):
        config = dict([(key, value) for key, value in self.options.items()
                       if key in self.cfg.settings and value is not None])
        for key, value in config.items():
            self.cfg.set(key.lower(), value)

    def load(self):
        return util.import_app("container_worker.dc_web")


def main(settings):
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
        dc_download(additional_settings['input_files'], additional_settings['input_file_keys'])
    except:
        description = 'Could not retrieve input files.'
        callback_handler.send_callback(
            callback_type='files_retrieved', state='failed', description=description, exception=format_exc()
        )
        exit(3)

    description = 'Input files available.'
    callback_handler.send_callback(callback_type='files_retrieved', state='success', description=description)

    options = {
        'bind': '0.0.0.0:80',
        'workers': cpu_count()
    }

    WebApp(options).run()

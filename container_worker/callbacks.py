import requests
from json import dumps


STATES = [
    'created',
    'waiting',
    'processing',
    'success',          # end state
    'failed',           # end state
    'cancelled'         # end state
]

AC_CALLBACK_TYPES = [
    'started',
    'files_retrieved',
    'processed',
    'results_sent'
]

DC_CALLBACK_TYPES = [
    'started',
    'files_retrieved'
]


def state_to_index(state):
    for i, s in enumerate(STATES):
        if s == state:
            return i
    raise Exception('Invalid state: %s' % str(state))


def callback_type_to_index(callback_type, callback_type_list):
    for i, t in enumerate(callback_type_list):
        if t == callback_type:
            return i
    raise Exception('Invalid callback_type: %s' % str(callback_type))


def callback_prototype():
    return {
        'callback_key': None,
        'callback_type': 0,
        'container_id': None,
        'content': {
            'state': 0,
            'description': None,
            'exception': None,
            'telemetry': None
        }
    }


class CallbackHandler:
    def __init__(self, settings):
        self.callback = callback_prototype()
        self.callback['callback_key'] = settings['callback_key']
        self.callback['container_id'] = settings['container_id']

        self.callback_url = settings['callback_url']
        self.callback_type_list = DC_CALLBACK_TYPES if settings['container_type'] == 'data' else AC_CALLBACK_TYPES

    def send_callback(self, callback_type, state, description, exception=None, telemetry=None):

        self.callback['callback_type'] = callback_type_to_index(
            callback_type,
            self.callback_type_list
        )

        self.callback['content']['state'] = state_to_index(state)
        self.callback['content']['description'] = description
        self.callback['content']['exception'] = exception
        self.callback['content']['telemetry'] = telemetry

        r = requests.post(
            self.callback_url,
            data=dumps(self.callback),
            headers={'Content-type': 'application/json', 'Accept': 'text/plain'}
        )

        r.raise_for_status()

        try:
            return r.json()
        except:
            pass

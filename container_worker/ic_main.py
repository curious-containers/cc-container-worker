import requests


def main(settings):
    requests.get(settings['inspection_url'])

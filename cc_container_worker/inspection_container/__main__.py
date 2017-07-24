import sys
import json
import requests


def main():
    settings = json.loads(sys.argv[1])
    requests.get(settings['inspection_url'])


if __name__ == '__main__':
    main()

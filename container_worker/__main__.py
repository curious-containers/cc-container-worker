import sys
import json

from container_worker import ac_main, dc_main

debug = False
settings_index = 1
if len(sys.argv) > 2 and sys.argv[1] in ['--debug', '-d']:
    debug = True
    settings_index = 2

settings = json.loads(sys.argv[settings_index])

if settings['container_type'] == 'data':
    dc_main.main(settings)
elif settings['container_type'] == 'application':
    ac_main.main(settings, debug)
else:
    exit(42)

import sys
import json

from container_worker import ac_main, dc_main, ic_main

settings = json.loads(sys.argv[1])

if settings['container_type'] == 'data':
    dc_main.main(settings)
elif settings['container_type'] == 'application':
    ac_main.main(settings)
elif settings['container_type'] == 'inspection':
    ic_main.main(settings)
else:
    exit(42)

import sys
from json import loads

from container_worker import ac_main, dc_main

settings = loads(sys.argv[1])

if settings['is_data_container']:
    dc_main.main(settings)
else:
    ac_main.main(settings)

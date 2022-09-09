from handler.testapi_handler import *


url = [
    (r'/api/ivsmile', ivsmile),
    (r'/api/ivterm', ivtermstructure),
    (r'/api/ivsurface', ivsurface),

    (r'/dev/api/ivsmile', dev_ivsmile),
    (r'/dev/api/ivterm', dev_ivtermstructure),
    (r'/dev/api/ss300', dev_ss300), # default: count = 0

    (r'/dev/api/spy', dev_SPY), # default: count = 0
    (r'/dev/apifull/spy', dev_full_SPY), # default: count = 0

]

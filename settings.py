import os

import requests
from feed.settings import nanny_params

params = requests.get("http://{host}:{port}/{params_manager}/getParameter/leader/{name}".format(**nanny_params, name=os.getenv("NAME")))

feed_params = params.json()

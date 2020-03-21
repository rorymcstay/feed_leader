import logging
import os
import signal
import sys
import argparse
from time import time, sleep

import requests
from selenium.common.exceptions import WebDriverException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

from feed.settings import nanny_params, routing_params
from settings import feed_params
from src.main.exceptions import NextPageException
from src.main.manager import FeedManager
from src.main.market.utils.WebCrawlerConstants import WebCrawlerConstants
from feed.service import Client
start = logging.getLogger("startup")

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
logging.FileHandler('/var/tmp/myapp.log')
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("kafka").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("selenium").setLevel(logging.WARNING)


parser = argparse.ArgumentParser(description='feed leader')
parser.add_argument('--run',action='store_true', default=False)
parser.add_argument('--test',action='store_true', default=False)
parser.add_argument('--single',action='store_true', default=False)



feed: FeedManager = FeedManager()

# check depepndent containers
nanny = Client("nanny", **nanny_params)
router = Client("routing", **routing_params)

# parse args
args = parser.parse_args()

if args.single:
    feed.singleMode()
elif args.test:
    feed.runMode(test=True)
else:
    feed.runMode()



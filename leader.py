import logging
import os
import signal
import sys
import argparse
from time import time, sleep
import time
import threading
import subprocess
import requests
from feed.settings import nanny_params, routing_params, command_params
from src.main.exceptions import NextPageException
from src.main.manager import FeedManager
from src.main.market.utils.WebCrawlerConstants import WebCrawlerConstants
from feed.service import Client
import json
from feed.crawling import beginBrowserThread
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
parser.add_argument('--name', default='donedeal')
parser.add_argument('--workerMode', action='store_true', default=False)
parser.add_argument('--startBrowser', action='store_true', default=False)


if __name__ == "__main__":


    args = parser.parse_args()
    if args.startBrowser:
        browser_thread = beginBrowserThread()

    feed: FeedManager = FeedManager()

    # check depepndent containers
    nanny = Client("nanny", **nanny_params)
    router = Client("routing", **routing_params)
    commands = Client("commands", **command_params)

    logging.info("nanny: {}".format(json.dumps(nanny_params, indent=4, sort_keys=True)))

    os.environ['name'] = args.name

    logging.info(f'starting {os.environ["name"]} feed leader')


    if args.workerMode:
        feed.workerMode()
    elif args.single:
        feed.singleMode()
    elif args.test:
        feed.runMode(test=True)
    else:
        feed.runMode()



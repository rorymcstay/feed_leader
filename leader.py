import logging
import os
import argparse
from feed.service import Client
from feed.crawling import beginBrowserThread
from feed.actionchains import KafkaActionSubscription, KafkaActionPublisher
from feed.crawling import BrowserService, BrowserActions
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


class LeaderCrawler(KafkaActionSubscription, BrowserService, KafkaActionPublisher):

    def __init__(self):
        KafkaActionSubscription.__init__(self, topic='leader-route', implementation=BrowserActions)
        BrowserService.__init__(self)
        KafkaActionPublisher.__init__(self)

    def onClickActionCallback(self, actionReturn: BrowserActions.Return):
        logging.info(f'onClickActionCallback')

    def onInputActionCallback(self, actionReturn: BrowserActions.Return):
        logging.info(f'onInputActionCallback')

    def onPublishActionCallback(self, actionReturn: BrowserActions.Return):
        self.rePublish(actionReturn)

    def onCaptureActionCallback(self, actionReturn: BrowserActions.Return):
        self.rePublish(actionReturn)


if __name__ == "__main__":
    args = parser.parse_args()
    if args.startBrowser:
        bt = beginBrowserThread()
    feed = LeaderCrawler()
    feed.main()


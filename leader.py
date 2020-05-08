import logging
import os
import argparse
from feed.service import Client
from feed.actionchains import KafkaActionSubscription, KafkaActionPublisher
from feed.crawling import BrowserService, BrowserActions
start = logging.getLogger("startup")


logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
logging.FileHandler('/var/tmp/myapp.log')
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("kafka").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("selenium").setLevel(logging.WARNING)


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
        logging.info(f'LeaderCrawler::onCaptureActionCallback(): chainName=[{actionReturn.name}], captureName=[{actionReturn.action.captureName}]')
        logging.debug(f'LeaderCrawler::onCaptureActionCallback(): data=[{str(actionReturn.data)}]')
        self.rePublish(actionReturn)

    def cleanUp(self):
        self._browser_clean_up()


if __name__ == "__main__":
    feed = LeaderCrawler()
    feed.main()


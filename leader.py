import logging
import os
from logging.config import dictConfig
import argparse
from feed.service import Client
from feed.actionchains import KafkaActionSubscription, KafkaActionPublisher
from feed.crawling import BrowserService, BrowserActions
from feed.settings import nanny_params, logger_settings_dict

class LeaderCrawler(KafkaActionSubscription, BrowserService, KafkaActionPublisher):

    def __init__(self):
        queue = f'leader-route'
        logging.info(f'subscribing to {queue}')
        KafkaActionSubscription.__init__(self, topic=queue, implementation=BrowserActions)
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
    dictConfig(logger_settings_dict('root')
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("kafka").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.ERROR)
    logging.getLogger("selenium").setLevel(logging.WARNING)
    logging.info("\n".join([f'{key}={os.environ[key]}' for key in os.environ]))
    feed = LeaderCrawler()
    feed.main()


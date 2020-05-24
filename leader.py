import logging
import os
from logging.config import dictConfig
import argparse
from feed.service import Client
from feed.actionchains import KafkaActionSubscription, KafkaActionPublisher
from feed.crawling import BrowserService, BrowserActions
start = logging.getLogger("startup")

class LeaderCrawler(KafkaActionSubscription, BrowserService, KafkaActionPublisher):

    def __init__(self):
        queue = f'{os.getenv("KAFKA_TOPIC_PREFIX", "u")}-leader-queue'
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
    dictConfig({
        'version': 1,
        'formatters': {'default': {
            'format': '[%(asctime)s]%(thread)d: %(module)s - %(levelname)s - %(message)s',
        }},
        'handlers': {'wsgi': {
            'class': 'logging.StreamHandler',
            'stream': 'ext://flask.logging.wsgi_errors_stream',
            'formatter': 'default'
        }},
        'root': {
            'level': 'INFO',
            'handlers': ['wsgi']
        }
    })
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("kafka").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.ERROR)
    logging.getLogger("selenium").setLevel(logging.WARNING)
    logging.info("\n".join([f'{key}={os.environ[key]}' for key in os.environ]))
    feed = LeaderCrawler()
    feed.main()


import logging
from feed.actionchains import KafkaActionPublisher,KafkaActionSubscription
from feed.crawling import BrowserService

class FeedManager(KafkaActionSubscription, BrowserService, KafkaActionPublisher):

    def __init__(self):
        KafkaActionPublisher.__init__(self)
        BrowserService.__init__(self)
        KafkaActionSubscription.__init__(self, topic='leader-route', implementation=BrowserActions)

    def onClickActionCallback(self, actionReturn: BrowserActions.Return):
        logging.info(f'onClickActionCallback')

    def onInputActionCallback(self, actionReturn: BrowserActions.Return):
        logging.info(f'onInputActionCallback')

    def onPublishActionCallback(self, actionReturn: BrowserActions.Return):
        self.rePublish(actionReturn)

    def onCaptureActionCallback(self, actionReturn: BrowserActions.Return):
        self.rePublish(actionReturn)


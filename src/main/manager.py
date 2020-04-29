import sys
import itertools
import json
from json.decoder import JSONDecodeError
from time import time
import os
import bs4
import requests as r
import logging as log
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import time
from time import sleep
from feed.settings import kafka_params, routing_params, retry_params, nanny_params
from feed.logger import getLogger
from src.main.crawling import WebCrawler
from selenium.common.exceptions import WebDriverException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

from feed.settings import nanny_params, routing_params
from src.main.exceptions import NextPageException
from src.main.market.utils.WebCrawlerConstants import WebCrawlerConstants
from feed.service import Client

import signal
start = log.getLogger("startup")

logging = getLogger(__name__)


class GracefulKiller:
    kill_now = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        self.kill_now = True

class FeedManager:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(FeedManager, cls).__new__(
                cls, *args, **kwargs)
        return cls._instance

    make = None
    model = None
    sort = None

    results = None
    results_batched = None
    busy = False
    browsers = []

    def registerFeed():
        req = r.get("http://{host}:{port}/{params_manager}/getParameter/leader/{name}".format(name=name, **nanny_params))

    def reloadFeedParams(self, name):
        try:
            req = r.get("http://{host}:{port}/{params_manager}/getParameter/leader/{name}".format(name=name, **nanny_params))
            self.feed_params = req.json()
            self.webCrawler.updateFeedParams(self.feed_params)
            logging.info(f'loaded feed_params={json.dumps(self.feed_params)}')
        except JSONDecodeError as ex:
            self.feed_params = {}
            logging.info(f'error, parsing parameter for {name}: lineno={ex.lineno}, msg="{ex.msg}"')

    def __init__(self, attempts=0):
        self.webCrawler = WebCrawler()
        try:
            self.kafkaProducer = KafkaProducer(**kafka_params)
        except NoBrokersAvailable as e:
            logging.warning(f'Kafka is not available, {e.args}')
            logging.warning(json.dumps(kafka_params, indent=4))

    def renewWebCrawler(self):
        logging.info("renewing webcrawler")
        self.webCrawler = WebCrawler()

    def goToLast(self):
        """
        navigate back to the base url, router sends a string like "url,multiple"
        :return:
        """

        home = r.get("http://{host}:{port}/{api_prefix}/getLastPage/{feedName}".format(feedName=self.feed_params.get('name'), **routing_params, **self.feed_params))
        logging.info(f'last page is {home}')

        if home.text == "none":
            home = "http://{host}:{port}/{api_prefix}/getResultPageUrl/{name}".format(**routing_params, **self.feed_params)
            home = r.get(home, data=json.dumps(dict(make=self.make,
                                                    model=self.model,
                                                    sort=self.sort)),
                         headers={"content-type": "application/json"})
            url = home.text
            logging.info(f'home is {url}')
        elif self.feed_params["page_url_param"].upper() in home.json()["url"].upper():
            data = home.json()
            url = str(data["url"])
            split = url.split("=")
            logging.debug(f'parsing page number from last visited url')
            try:
                for (num, l) in enumerate(split):
                    if self.feed_params["page_url_param"].lower() in l.lower():
                        parsed = "".join(itertools.takewhile(str.isdigit, split[num + 1]))
                        self.webCrawler.page = int(int(parsed)/int(data["increment"]))
                        logging.info(f'{self.feed_params.get("name")}: Left off from, url={url} page={self.webCrawler.page}')
                        break
            except ValueError as e:
                logging.warning(f'{self.feed_params.get("name")}: failed to parse page number from: url={url}, increment={data["increment"]}, page_url_param={self.feed_params["page_url_param"]}')
                self.webCrawler.page = 0
        else:
            data = home.json()
            url = str(data["url"])

        try:
            self.webCrawler.driver.get(url)
            logging.info("navigated to {} ok".format(url))
        except WebDriverException as ex:
            logging.warning(f'{self.feed_params.get("name")}: WebDriverException going to the last page url={url}, exception={ex.args}')
            self.webCrawler.renewWebCrawler()
            self.webCrawler.get(url)
        return home

    def goToLastPending(self, make=None, model=None, sort='newest'):
        self.make = make
        self.model = model
        self.sort = sort
        self.goToLast()
        try:
            self.webCrawler.nextPage()
        except NextPageException:
            self.goToNextWithRouter()

    def publishListOfResults(self):
        stream = self.feed_params['result_stream'].get("class")
        parser = bs4.BeautifulSoup(self.webCrawler.driver.page_source, features="html.parser")
        results = parser.findAll(attrs={"class": stream})
        logging.debug("parsed {} results from {}".format(len(results), self.webCrawler.driver.current_url))
        i = 0
        for result in results:
            data = dict(value=bytes(str(result), 'utf-8'),
                        key=bytes("{}_{}".format(self.webCrawler.driver.current_url, i), 'utf-8'))
            self.kafkaProducer.send(topic="{name}-results".format(**self.feed_params), **data)
            i += 1
        logging.info("parsed {} results from {}".format(len(results), self.webCrawler.driver.current_url))

    def goToNextWithRouter(self):
        nextPage = "http://{host}:{port}/{api_prefix}/getResultPageUrl/{name}".format(**routing_params,
                                                                                      **self.feed_params)
        nextPage = r.get(nextPage, json=dict(make=self.make,
                                                    model=self.model,
                                                    sort=self.sort,
                                                    page=self.webCrawler.page))
        try:
            self.webCrawler.driver.get(nextPage.text)
        except TimeoutException:
            self.webCrawler.driver.get(nextPage.text)
        try:
            element_present = EC.presence_of_element_located((By.CSS_SELECTOR, self.feed_params['wait_for']))
            WebDriverWait(self.webCrawler.driver, WebCrawlerConstants().click_timeout).until(element_present)
        except TimeoutException:
            logging.warning("page did not load as expected - timeout exception")

    def runMode(self, test=False):
        name = os.environ['NAME']
        self.reloadFeedParams(name)
        self.goToLastPending()
        killer = GracefulKiller()
        start.info("leader has started")
        it = 0
        while (not test) or (it < 2):
            timeStart = time.time()
            it +=1
            try:
                self.publishListOfResults()
                try:
                    self.webCrawler.nextPage()
                except (NextPageException, WebDriverException) as e:
                    logging.warning("{} raised whilst going to next page - using router".format(e))
                    self.goToNextWithRouter()
            except TypeError as e:
                logging.warning("type error - {}".format(e.args[0]))
                self.webCrawler.driver.refresh()
            except TimeoutException as e:
                logging.info("Webdriver timed out")

            logging.info(msg="published page to kafka results in {}".format(time.time() - timeStart))
            r.put("http://{host}:{port}/{api_prefix}/updateHistory/{name}".format(name=self.feed_params["name"],
                                                                                         **routing_params),
                         data=self.webCrawler.driver.current_url)

            sleep(5)
            # verify then wait until page ready

            if killer.kill_now:
                self.webCrawler.driver.close()
                r.get(
                    "http://{host}:{port}/{api_prefix}/freeContainer/{free_port}".format(free_port=self.webCrawler.port,
                                                                                          **nanny_params))
                sys.exit()

    def singleMode(self, name=os.getenv('NAME', None), runTimes=1):
        i = 0
        while i < runTimes:
            i += 1
            self.reloadFeedParams(name)
            self.goToLastPending()
            self.publishListOfResults()
            r.put("http://{host}:{port}/{api_prefix}/updateHistory/{name}".format(name=self.feed_params["name"],
                                                                                         **routing_params),
                         data=self.webCrawler.driver.current_url)

    def workerMode(self):
        self.running = True
        while self.running:
            name = r.get('http://{host}:{port}/runningmanager/getNextFeed/'.format(**nanny_params)).json()
            if name.get('name') == None:
                logging.info(f'nothing to do. will wait')
                sleep(10)
            else:
                r.get('http://{host}:{port}/runningmanager/markRunning/{name}'.format(name=name.get('name'), **nanny_params))
                self.singleMode(name=name.get('name'), runTimes=2)
                r.get('http://{host}:{port}/runningmanager/markDone/{name}'.format( name=self.feed_params.get('name'), **nanny_params))


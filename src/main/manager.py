import sys
import itertools
import json
import logging
from time import time
import bs4
import requests as r
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import time
from time import sleep
from feed.settings import kafka_params, routing_params, retry_params
from settings import feed_params
from src.main.crawling import WebCrawler
from selenium.common.exceptions import WebDriverException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

from feed.settings import nanny_params, routing_params
from settings import feed_params
from src.main.exceptions import NextPageException
from src.main.market.utils.WebCrawlerConstants import WebCrawlerConstants
from feed.service import Client

import signal
start = logging.getLogger("startup")

log = logging.getLogger(__name__)

start = logging.getLogger("startup")

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


    def __init__(self, attempts=0):
        self.webCrawler = WebCrawler()

        try:
            self.kafkaProducer = KafkaProducer(**kafka_params)
        except NoBrokersAvailable as e:
            logging.info("Kafka is not available")
            logging.info(json.dumps(kafka_params, indent=4))



    def renewWebCrawler(self):
        logging.info("renewing webcrawler")
        self.webCrawler = WebCrawler()

    def goHome(self):
        """
        navigate back to the base url, router sends a string like "url,multiple"
        :return:
        """

        home = r.get("http://{host}:{port}/{api_prefix}/getLastPage/{name}".format(**routing_params, **feed_params))

        if home.text == "none":
            home = "http://{host}:{port}/{api_prefix}/getResultPageUrl/{name}".format(**routing_params, **feed_params)
            home = r.get(home, data=json.dumps(dict(make=self.make,
                                                    model=self.model,
                                                    sort=self.sort)),
                         headers={"content-type": "application/json"})
            url = home.text
        elif feed_params["page_url_param"].upper() in home.json()["url"].upper():
            data = home.json()
            url = str(data["url"])
            split = url.split("=")
            try:
                for (num, l) in enumerate(split):
                    if feed_params["page_url_param"].lower() in l.lower():
                        parsed = "".join(itertools.takewhile(str.isdigit, split[num + 1]))
                        self.webCrawler.page = int(int(parsed)/int(data["increment"]))
                        break
            except ValueError as e:
                self.webCrawler.page = 0
        else:
            data = home.json()
            url = str(data["url"])
        logging.debug("navigating to starting point {}".format(url))
        self.webCrawler.driver.get(url)
        logging.info("navigated to {} ok".format(url))
        return home

    def setHome(self, make=None, model=None, sort='newest'):
        self.make = make
        self.model = model
        self.sort = sort
        return self.goHome()

    def publishListOfResults(self):
        stream = feed_params['result_stream'].get("class")
        parser = bs4.BeautifulSoup(self.webCrawler.driver.page_source, features="html.parser")
        results = parser.findAll(attrs={"class": stream})
        logging.debug("parsed {} results from {}".format(len(results), self.webCrawler.driver.current_url))
        i = 0
        for result in results:
            data = dict(value=bytes(str(result), 'utf-8'),
                        key=bytes("{}_{}".format(self.webCrawler.driver.current_url, i), 'utf-8'))
            self.kafkaProducer.send(topic="{name}-results".format(**feed_params), **data)
            i += 1
            logging.debug("published {} results".format(i))
        logging.info("parsed {} results from {}".format(len(results), self.webCrawler.driver.current_url))

    def runMode(self, test=False):
        self.setHome()
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
                    nextPage = "http://{host}:{port}/{api_prefix}/getResultPageUrl/{name}".format(**routing_params,
                                                                                                  **feed_params)
                    nextPage = r.get(nextPage, json=dict(make=self.make,
                                                                model=self.model,
                                                                sort=self.sort,
                                                                page=self.webCrawler.page))

                    try:
                        self.webCrawler.driver.get(nextPage.text)
                    except TimeoutException:
                        self.webCrawler.driver.get(nextPage.text)
                    try:
                        element_present = EC.presence_of_element_located((By.CSS_SELECTOR, feed_params['wait_for']))
                        WebDriverWait(self.webCrawler.driver, WebCrawlerConstants().click_timeout).until(element_present)
                    except TimeoutException:
                        logging.info("page did not load as expected - timeout exception")
            except TypeError as e:
                logging.warning("type error - {}".format(e.args[0]))
                self.webCrawler.driver.refresh()
            except TimeoutException as e:
                logging.info("Webdriver timed out")

            logging.info(msg="published page to kafka results in {}".format(time.time() - timeStart))
            r.put("http://{host}:{port}/{api_prefix}/updateHistory/{name}".format(name=feed_params["name"],
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
        self.webCrawler.driver.close()
        sys.exit()

    def singleMode(self):
        self.setHome()
        self.publishListOfResults()
        sys.exit()


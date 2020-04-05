import os
import re
import traceback
from http.client import RemoteDisconnected
from time import time, sleep

import bs4
import requests as r
import selenium.webdriver as webdriver
from bs4 import Tag
from selenium.common.exceptions import (TimeoutException,
                                        StaleElementReferenceException,
                                        WebDriverException)
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from urllib3.exceptions import MaxRetryError, ProtocolError

from feed.settings import browser_params, nanny_params
from feed.crawling import BrowserService
from src.main.exceptions import NextPageException
from src.main.market.utils.WebCrawlerConstants import WebCrawlerConstants
from feed.logger import getLogger

logging = getLogger(__name__)


class WebCrawler(BrowserService):
    driver: WebDriver

    def __init__(self, port=os.getenv("BROWSER_PORT")):
        """

        """
        super().__init__()

        self.driver.set_window_size(1020, 900)
        self.history = []
        self.page = 1

    def updateFeedParams(self, feed_params):
        self.feed_params = feed_params

    def safelyClick(self, item, wait_for, selector, timeout=3):
        """
        identifies xpath and css path to button. Attempts
        This function handles cases where the click is intercepted or the webpage did not load as expected
        :param selector:
        :param wait_for:
        :param item:
        :return:
        """

        try:
            item.click()
            element_present = EC.presence_of_all_elements_located((selector, wait_for))
            WebDriverWait(self.driver, timeout).until(element_present)
            return True
        except StaleElementReferenceException as e:
            logging.warning(
                msg="click failure - text: {button}, exception: {exception}".format(button=item.text, exception=e.msg))
            traceback.print_exc()
            return False
        except WebDriverException:

            traceback.print_exc()
            item = self.driver.find_element_by_css_selector(self.feed_params["next_page_css"])
            item.click()
            return False

    def resultPage(self):
        if self.feed_params['result_stub'] in self.driver.current_url:
            return False
        elif self.feed_params['base_url'] not in self.driver.current_url:
            reportParameter('base_url')
            return False
        else:
            return True

    def latestPage(self):
        try:
            self.driver.get(self.last_result)
            element_present = EC.presence_of_all_elements_located((By.CSS_SELECTOR, self.feed_params['wait_for']))
            WebDriverWait(self.driver, 5).until(element_present)
            return True
        except TimeoutException:
            logging.warning("{} did not load as expected or unusually slowly at {}".format(self.feed_params['wait_for'],
                                                                                           self.driver.current_url))
            return True

    def nextPage(self, attempts=0):
        logging.info("attempt {}: going to next page from {}".format(attempts, self.driver.current_url))
        if not attempts > 0:
            self.page += 1
        while attempts < WebCrawlerConstants().max_attempts:
            try:
                button = self.getNextButton()
                button.click()
                element_present = EC.presence_of_element_located((By.CSS_SELECTOR, self.feed_params['wait_for']))
                try:
                    WebDriverWait(self.driver, WebCrawlerConstants().click_timeout).until(element_present)
                except:
                    reportParameter(parameter_key='wait_for')
                    logging.info("page did not load as expected - timeout exception")
                if not self.resultPage():
                    raise NextPageException(self.page, "navigated to wrong page type")
            except AttributeError:

                logging.warning("couldn't find next button trying again {} more times".format(
                    attempts - WebCrawlerConstants().max_attempts))
                attempts += 1
                self.nextPage(attempts)
                return
            except WebDriverException as e:
                raise NextPageException(self.page, e.msg)
        raise NextPageException(self.page, "maximum attempts reached")

    def getNextButton(self) -> WebElement:
        buttons = self.driver.find_elements_by_xpath(self.feed_params['next_page_xpath'])
        for button in buttons:
            text = button.text.upper()
            if self.feed_params['next_button_text'].upper() in text:
                return button
        logging.debug("couldn't find next button by xpath - found {} buttons".format(len(buttons)))
        reportParameter('next_page_xpath')
        # 1st fallback is to use css
        buttons = self.driver.find_elements_by_css_selector(self.feed_params.get("next_page_css"))
        for button in buttons:
            text = button.text.upper()
            if self.feed_params.get("next_button_text").upper() in text:
                return button
        logging.debug("couldn't find next button by css selector - found {} buttons".format(len(buttons)))
        reportParameter("next_page_css")

    def parseNextButton(self) -> Tag:
        item: Tag = bs4.BeautifulSoup(self.driver.page_source,
                                      features='html.parser').find(attrs={"class": re.compile('.*(?i)pagination.*')})
        try:
            next = item.findAll(attrs={"class": re.compile('.*(?i)next.*')})
            for item in next:
                if self.feed_params['next_button_text'].upper() in item.text.upper():
                    logging.info("found button {}".format(item.text))
                    return item
        except AttributeError:
            logging.info("next button not found")

    def getResultList(self):
        start = time()
        content = self.driver.page_source
        cars = []
        cars.extend(re.findall(r'' + self.feed_params['result_stub'] + '[^\"]+', content))
        logging.debug(
            "parsed result array of length {length} in {time} s".format(length=len(cars), time=time() - start))
        return list(set(cars))

    def retrace_steps(self, x):
        self.driver.get(self.feed_params['home'])
        WebDriverWait(self.driver, 2)
        page = 1
        while page < x:
            self.nextPage()
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, self.feed_params['next_page_xpath'])))
            page = page + 1
            logging.info(page)

    def healthIndicator(self):
        try:
            return self.driver.current_url
        except (WebDriverException, Exception) as e:
            return 'Unhealthy'

    def quit(self):
        try:
            self.driver.quit()
        except MaxRetryError as e:
            logging.error("Tried to close browser when it was not running.")


def reportParameter(parameter_key=None):
    endpoint = "http://{host}:{port}/parametermanager/reportParameter/{}/{}/{}".format(
        os.getenv("NAME"),
        parameter_key,
        "leader",
        **nanny_params
    )
    r.get(endpoint)

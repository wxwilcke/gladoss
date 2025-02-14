#!/usr/bin/env python

from abc import ABC, abstractmethod
from collections.abc import Iterable
import logging
import json
import sys
from time import sleep
from typing import Optional, Self

from rdf import Statement
import requests

logger = logging.getLogger(__name__)


class Adaptor(ABC):
    """ Abstract Base Class for adaptors. Subclass this class with a custom
        translation function to receive data from IoT devices via a RESTful API
        and to then convert these data to a corresponding RDF graph.
    """

    def __init__(self: Self, address: str = "http://127.0.0.1:8000",
                 continuous: bool = False, num_retries: int = 3,
                 retry_delay: float = 30., request_delay: float = 0.5) -> None:
        self.address = address
        self.continuous = continuous
        self.num_retries = num_retries
        self.retry_delay = retry_delay
        self.request_delay = request_delay

    @abstractmethod
    def translate(self: Self, data: dict[str, str],
                  **kwargs: Optional[str]) -> list[Statement]:
        """ Translate the received data to RDF.

        :param data: data received from API
        :param kwargs: optional keyword arguments
        :return: a list with statements
        """
        pass

    def _wait_on_error(self: Self, retries: int) -> None:
        """ Wait a number of seconds after experiencing an error
            before trying again.

        :param retries: The number of retries until now.
        """
        info = f"{retries+1}" if self.continuous\
            else f"{retries+1}/{self.num_retries}"
        logger.debug(f"Retrying ({info}) in {self.retry_delay} seconds")

        # account for request interval
        delay = self.retry_delay - self.request_delay
        if delay < 0:
            delay = self.request_delay

        sleep(delay)

    def poll(self: Self,
             session: requests.Session,
             address: str) -> tuple[int, str]:
        """ Poll server exactly once.

        :param session: Open session with server
        :param address: HTTP address to listen to
        :return: A tuple with the response HTTP status and the content
        """
        response = session.get(address)

        return response.status_code, response.text

    def listen(self) -> Iterable[list[Statement]]:
        """ Listen at the provided address for changes in the message,
            and return the updates once successfully received. Terminates
            or retries when receiving a 204 or 408 HTTP status.

        :return: A received item as a dictionary.
        """
        retries = 0
        session = requests.Session()
        while True:
            logging.debug("Polling server")
            try:
                status_code, data_raw = self.poll(session, self.address)
            except requests.exceptions.RequestException:
                logging.debug("Connection Error")
                print("Cannot establish connection to server")
                sys.exit(1)

            logger.debug(f"Responded HTTP status: {status_code}")
            if status_code == 200:  # OK
                try:
                    data = json.loads(data_raw)  # dict[str, str]

                    yield self.translate(data)
                except json.JSONDecodeError:
                    logger.exception("JSONDecodeError on {data_raw}")

                    if not self.continuous and retries >= self.num_retries:
                        logging.debug("Reached maximum number of retries")
                        break

                    self._wait_on_error(retries)
                    retries += 1

            if status_code == 204:  # no content
                logger.info("Device reports no further content")
                if not self.continuous:
                    break

            if status_code >= 400:  # client or server error
                logger.debug("Error with request or response")
                if not self.continuous and retries >= self.num_retries:
                    logging.debug("Reached maximum number of retries")
                    break

                self._wait_on_error(retries)

                retries += 1

            sleep(self.request_delay)

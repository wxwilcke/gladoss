#!/usr/bin/env python

from collections.abc import Iterable
import logging
import json
import sys
from threading import Event
from typing import Any, Optional, Self

from rdf import Statement, IRIRef
from rdf.terms import Resource
import requests

from gladoss.adaptors.adaptor import Adaptor


logger = logging.getLogger(__name__)


class Monitor():
    def __init__(self: Self, controller: Event,  adaptor: Adaptor,
                 endpoint: str = "http://127.0.0.1:8000",
                 continuous: bool = False, num_retries: int = 3,
                 retry_delay: float = 30., request_delay: float = 0.5) -> None:
        self.adaptor = adaptor
        self.endpoint = endpoint
        self.continuous = continuous
        self.num_retries = num_retries
        self.retry_delay = retry_delay
        self.request_delay = request_delay
        self._controller = controller

        adaptor.init_hook()

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

        self._controller.wait(delay)

    def poll(self: Self,
             session: requests.Session,
             endpoint: str,
             headers: dict,
             data: dict) -> tuple[int, str]:
        """ Poll server exactly once.

        :param session: Open session with server
        :param endpoint: HTTP endpoint to listen to
        :return: A tuple with the response HTTP status and the content
        """
        response = session.post(endpoint, headers=headers, json=data)

        return response.status_code, response.text

    def listen(self) -> Iterable[tuple[list[Statement], list[Resource]]]:
        """ Listen at the provided endpoint for changes in the message,
            and return the updates once successfully received. Terminates
            or retries when receiving a 204 or 408 HTTP status.

        :return: A received item as a dictionary.
        """
        retries = 0

        package_headers = self.adaptor.set_headers()
        package_data = self.adaptor.set_payload()
        session = requests.Session()
        while not self._controller.is_set():
            logging.debug("Polling server")
            try:
                status_code, data_raw = self.poll(session,
                                                  self.endpoint,
                                                  package_headers,
                                                  package_data)
            except requests.exceptions.RequestException:
                logging.debug("Connection Error")
                print("Cannot establish connection to server")
                sys.exit(1)

            logger.debug(f"Responded HTTP status: {status_code}")
            if status_code == 200:  # OK
                try:
                    data = json.loads(data_raw)  # dict[str, Any]

                    message = self.adaptor.translate(data)
                    anchors = self.adaptor.get_anchors(data)

                    yield (message, anchors)
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

            self._controller.wait(self.request_delay)

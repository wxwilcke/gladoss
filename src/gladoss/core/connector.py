#!/usr/bin/env python

import logging
import json
from typing import Any, Generator, Self

from rdf import Statement
import requests

from gladoss.adaptors.adaptor import Adaptor


logger = logging.getLogger(__name__)


class Connector():
    def __init__(self: Self, adaptor: Adaptor,
                 endpoint: str = "http://127.0.0.1:8000",
                 continuous: bool = False, num_retries: int = 3,
                 retry_delay: float = 30., request_delay: float = 0.5,
                 return_receipt: bool = False) -> None:
        """ A connector connects an adaptor to an endpoint and listens
            for new messages, which get passed to the adaptor for translation
            before returning the result. Returns a receipt on receiving a
            new message if requested.

        :param adaptor: [TODO:description]
        :param endpoint: [TODO:description]
        :param continuous: [TODO:description]
        :param num_retries: [TODO:description]
        :param retry_delay: [TODO:description]
        :param request_delay: [TODO:description]
        :param return_receipt: [TODO:description]
        """
        self.adaptor = adaptor
        self.endpoint = endpoint
        self.continuous = continuous
        self.num_retries = num_retries
        self.retry_delay = retry_delay
        self.request_delay = request_delay
        self.return_receipt = return_receipt

        self.session = requests.Session()

    def _wait_on_error(self: Self, retries: int) -> None:
        """ Wait a number of seconds after experiencing an error
            before trying again.

        :param retries: The number of retries until now.
        """
        info = f"{retries+1}" if self.continuous\
            else f"{retries+1}/{self.num_retries}"
        logger.debug(f"Retrying in {self.retry_delay} seconds ({info})")

        # account for request interval
        delay = self.retry_delay - self.request_delay
        if delay < 0:
            delay = self.request_delay

        self.adaptor._controller.wait(delay)

    def poll(self: Self,
             session: requests.Session,
             endpoint: str,
             headers: dict[str, Any],
             data: dict[str, Any]) -> tuple[int, str]:
        """ Poll server exactly once.

        :param session: Open session with server
        :param endpoint: HTTP endpoint to listen to
        :return: A tuple with the response HTTP status and the content
        """
        response = session.get(endpoint, headers=headers, json=data)

        return response.status_code, response.text

    def push(self: Self,
             session: requests.Session,
             endpoint: str,
             headers: dict[str, Any],
             data: dict[str, Any]) -> int:
        """ Push message to server.

        :param session: Open session with server
        :param endpoint: HTTP endpoint to listen to
        :return: The response HTTP status
        """
        response = session.post(endpoint, headers=headers, json=data)

        return response.status_code

    def listen(self) -> Generator[tuple[str, list[Statement]], None, None]:
        """ Listen at the provided endpoint for changes in the message,
            and return the updates once successfully received. Terminates
            or retries when receiving a 204 or 408 HTTP status.

        :return: A received item as a dictionary.
        """
        retries = 0

        package_headers = self.adaptor.set_headers()
        package_payload = self.adaptor.set_payload()
        while not self.adaptor._controller.is_set():
            logging.debug("Polling server")
            try:
                status_code, data_raw = self.poll(self.session,
                                                  self.endpoint,
                                                  package_headers,
                                                  package_payload)
            except requests.exceptions.RequestException:
                logger.info("Cannot establish connection to server "
                            f"{self.endpoint}")

                if not self.continuous and retries >= self.num_retries:
                    logging.debug("Reached maximum number of retries")
                    break

                self._wait_on_error(retries)
                retries += 1

                continue

            logger.debug(f"Responded HTTP status: {status_code}")
            if status_code == requests.codes.ok:  # 200: ok
                try:
                    data = json.loads(data_raw)  # dict[str, Any]

                    # Optionally answer the poll response
                    if self.return_receipt:
                        logger.debug("Sending message receipt to endpoint "
                                     f"{self.endpoint})")
                        ack_headers = self.adaptor.set_receipt_headers(data)
                        ack_payload = self.adaptor.set_receipt_payload(data)
                        try:
                            self.push(self.session,
                                      self.endpoint,
                                      ack_headers,
                                      ack_payload)
                        except requests.exceptions.RequestException:
                            logger.warning("Error on sending message receipt")

                    # reset the number of retries on success
                    retries = 0

                    for message in self.adaptor.translate(data):
                        yield message
                except json.JSONDecodeError:
                    logger.exception("JSONDecodeError on {data_raw}")

                    if not self.continuous and retries >= self.num_retries:
                        logging.debug("Reached maximum number of retries")
                        break

                    self._wait_on_error(retries)
                    retries += 1

                    continue

            if status_code == requests.codes.accepted:  # 202: poll timeout
                # renew poll; don't count this as a retry
                continue

            if status_code == requests.codes.no_content:  # 204: no content
                logger.info("Device reports no further content")
                if not self.continuous:
                    break

            if status_code >= requests.codes.bad_request:  # 400: server error
                logger.debug("Error with request or response")
                if not self.continuous and retries >= self.num_retries:
                    logging.debug("Reached maximum number of retries")
                    break

                self._wait_on_error(retries)

                retries += 1

            self.adaptor._controller.wait(self.request_delay)

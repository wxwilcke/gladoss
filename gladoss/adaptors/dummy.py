#!/usr/bin/env python

"""
Adaptor to dummy device for debugging purposes
"""

import argparse
from collections.abc import Iterable
import logging
import json
import sys
from time import sleep

from fastapi import status
import requests

logger = logging.getLogger(__name__)


def poll(session: requests.Session,
         address: str) -> tuple[int, str]:
    """ Poll server exactly once.

    :param session: Open session with server
    :param address: HTTP address to listen to
    :return: A tuple with the response HTTP status and the content
    """
    response = session.get(address)

    return response.status_code, response.text


def wait_on_error(num_retries: int, flags: argparse.Namespace):
    """ Wait a number of seconds after experiencing an error
        before trying again.

    :param num_retries: The number of retries until now.
    :param flags: User-provided parameters
    """
    if flags.continuous:
        logger.debug(f"Retrying (#{num_retries+1}) in "
                     f"{flags.retry_delay} seconds")
    else:
        logger.debug(f"Retrying ({num_retries+1}/{flags.retries}) in "
                     f"{flags.retry_delay} seconds")

    # account for request interval
    delay = flags.retry_delay - flags.request_delay
    if delay < 0:
        delay = flags.request_delay

    sleep(delay)


def listen(flags: argparse.Namespace) -> Iterable[dict[str, str]]:
    """ Listen at the provided address for changes in the message,
        and return the updates once successfully received. Terminates
        or retries when receiving a 204 or 408 HTTP status.

    :param flags: User-provided parameters.
    :return: A received item as a dictionary.
    """
    num_retries = 0
    session = requests.Session()
    while True:
        logging.debug("Polling server")
        try:
            status_code, data_raw = poll(session, flags.address)
        except requests.exceptions.RequestException:
            logging.debug("Connection Error")
            print("Cannot establish connection to server")
            sys.exit(1)

        logger.debug(f"Responded HTTP status: {status_code}")
        if status_code == status.HTTP_200_OK:
            try:
                data = json.loads(data_raw)  # dict[str, str]

                yield data
            except json.JSONDecodeError:
                logger.exception("JSONDecodeError on {data_raw}")

                if not flags.continuous and num_retries >= flags.retries:
                    logging.debug("Reached maximum number of retries")
                    break

                wait_on_error(num_retries, flags)
                num_retries += 1

        if status_code == status.HTTP_204_NO_CONTENT:
            logger.info("Device reports no further content")
            if not flags.continuous:
                break

        if status_code == status.HTTP_408_REQUEST_TIMEOUT:
            logger.debug("Request timed out")
            if not flags.continuous and num_retries >= flags.retries:
                logging.debug("Reached maximum number of retries")
                break

            wait_on_error(num_retries, flags)

            num_retries += 1

        sleep(flags.request_delay)


def main(flags: argparse.Namespace):
    # TODO: chance to class with abc template
    for item in listen(flags):
        print(item)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--address", help="HTTP address to listen to",
                        default="http://127.0.0.1:8000", type=str,
                        required=True)
    parser.add_argument("--continuous", help="Keep listening for changes in "
                        + "the response, irrespective of response status",
                        default=False, action="store_true")
    parser.add_argument("--retries", help="Number of retries on error.",
                        default=3, type=int)
    parser.add_argument("--retry_delay", help="Number of seconds to wait "
                        + "before retrying after the occurrence of an error",
                        default=30, type=int)
    parser.add_argument("--request_delay", help="Number of seconds to wait "
                        + "between polling the server.", default=0.5, type=int)
    parser.add_argument("--verbose", "-v", help="Show debug messages in "
                        + "console.", action='count', default=0)

    flags = parser.parse_args()

    # set log level
    log_level = logging.NOTSET
    if flags.verbose >= 2:
        log_level = logging.DEBUG
    elif flags.verbose == 1:
        log_level = logging.INFO
    else:
        log_level = logging.WARNING

    logging.basicConfig(level=log_level,
                        format='[%(asctime)s] [%(levelname)s] - %(message)s')

    main(flags)

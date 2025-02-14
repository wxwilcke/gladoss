#!/usr/bin/env python

"""
Start a standalone application to test server/client communication.
Pattern learning and anomaly detection will not occur.
"""

import argparse
import logging

from gladoss.adaptors.dummy import DummyAdaptor

logger = logging.getLogger(__name__)

LAND = "\u2227"  # unicode logical conjunction


def main(flags: argparse.Namespace):
    logging.info(f"Listening to {flags.address}")
    adaptor = DummyAdaptor(address=flags.address,
                           continuous=flags.continuous,
                           num_retries=flags.retries,
                           retry_delay=flags.retry_delay,
                           request_delay=flags.request_delay)

    for fact_lst in adaptor.listen():
        print(f" {LAND} ".join(fact_lst))


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

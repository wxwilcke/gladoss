#!/usr/bin/env python

"""
Start a standalone application to test server/client communication.
Pattern learning and anomaly detection will not occur.
"""

import argparse
import logging
import signal
from threading import Event

from gladoss.adaptors.dummy import DummyAdaptor

logger = logging.getLogger(__name__)

LAND = "\N{LOGICAL AND}"


def signal_handler(signum, frame):
    signal.signal(signum, signal.SIG_IGN)
    logger.info("Received Keyboard Interrupt")

    global controller
    controller.set()


def main(flags: argparse.Namespace):
    """ Call the appropriate adaptor and print any received message to stdout.

    :param flags: User-provided parameters
    """
    logging.info("Listening for messages")
    adaptor = DummyAdaptor(controller=controller,  # type: ignore
                           config=flags)

    conn = adaptor.connectors.pop()  # the demo only defines one connector
    for graph_id, graph in conn.listen():
        print(f" {LAND} ".join([str(fact) for fact in graph]))
        logger.debug(f"Graph Identity: {graph_id}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", help="HTTP address to listen to",
                        default="http://127.0.0.1:8000", type=str)
    parser.add_argument("--continuous", help="Keep listening for changes in "
                        + "the response, irrespective of response status",
                        default=False, action="store_true")
    parser.add_argument("--retries", help="Number of retries on error.",
                        default=3, type=int)
    parser.add_argument("--retry-delay", help="Number of seconds to wait "
                        + "before retrying after the occurrence of an error",
                        default=30, type=int)
    parser.add_argument("--return-receipt", help="Send acknowledgement "
                        + "to sender upon reception of message.",
                        action='store_true', default=False)
    parser.add_argument("--request-delay", help="Number of seconds to wait "
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
                        format='[%(asctime)s] [%(levelname)s] %(filename)s '
                               '- %(message)s')
    # register SIGINT signal handler
    global controller
    controller = Event()

    signal.signal(signal.SIGINT, signal_handler)

    main(flags)

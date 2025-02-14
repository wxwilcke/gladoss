#!/usr/bin/env python

"""
Dummy RESTful API for debugging purposes
"""

import argparse
from datetime import datetime
import logging
import json
from time import sleep, time
from threading import Thread
from queue import Queue
from random import randrange

from fastapi import FastAPI, Response, status, HTTPException
import uvicorn

app = FastAPI()
logger = logging.getLogger(__name__)


@app.get("/")
def publish(response: Response) -> dict[str, str]:
    """ Return published items on request if available. Use long polling to
    return update as soon as it becomes available.

    :return: a published item via a RESTful api
    """
    global cache

    if depleted:
        response.status_code = status.HTTP_204_NO_CONTENT
        return {"detail": "out of items"}

    t0 = time()
    while time() - t0 < timeout:
        if not cache.empty():
            item_cur = cache.get()  # dict[str, str]

            return item_cur

        sleep(1)

    logging.debug(f"Request timeout after {timeout} seconds")
    raise HTTPException(status_code=status.HTTP_408_REQUEST_TIMEOUT,
                        detail="Request timeout")


def cycleItems(data: list[dict[str, str]], flags: argparse.Namespace):
    """ Cycle through items, publishing each one after a random or fixed delay.
    Queue items if a cachesize > 1 is specified.

    :param data: list with items to publish
    :param flags: user-provided parameters
    """
    global cache, depleted
    for i, item in enumerate(data):
        if flags.realtime:
            # add or replace with current time
            item["timestamp"] = datetime.now().isoformat()

        logger.info(f"Publishing item {i+1} / {len(data)}: {item}")
        if not cache.full():
            cache.put(item)
        else:
            logger.debug("Cache is full")

        if i < len(data) - 1:
            delay = getDelay(flags.interval)
            logger.debug(f"Waiting for {delay} seconds")
            sleep(delay)

    logger.info("Out of items")
    depleted = True


def getDelay(interval: range) -> int:
    """ Return a random delay within the provided interval, or a fixed number
    of the start and stop values are the same.

    :param interval: a range of integers between which to sample
    :return: the delay in seconds
    """
    if interval.start == interval.stop:
        return interval.start

    return randrange(interval.start, interval.stop)


def integerRangeArg(arg: str) -> range:
    """ Custom argument type for range

    :param arg: user provided argument string of form 'from:to', ':to', or
        'to', with 'from' and 'to' being positive integers.
    :returns: range of values to explore
    """

    begin = 1
    arg_lst = arg.split(':')
    try:
        end = int(arg_lst[-1])
        if len(arg_lst) > 1 and len(arg_lst[0]) > 0:
            begin = int(arg_lst[0])
    except:
        raise Exception("'" + arg + "' is not a range of numbers. "
                        + "Expects '1:3', ':3', or '3'.")

    # check if range is valid
    assert begin >= 0 and end >= 0 and begin <= end

    return range(begin, end)


def load_json(filename: str) -> list[dict[str, str]]:
    """ Load JSON file with items to publish

    :param filename: the path to the JSON file to load
    :return: a list of items to publish
    """
    data = list()
    with open(filename, 'r') as f:
        data = json.load(f)

    return data


def main(flags: argparse.Namespace):
    """ Load data, initialise cache, and start new thread that cycles through
    the the provided tems.

    :param flags: user-provided parameters
    """
    global cache, depleted, timeout

    data = load_json(flags.input)  # list[dict[str, str]]
    cache = Queue(maxsize=flags.cachesize)
    timeout = flags.timeout
    depleted = False

    logger.info(f"Loaded {len(data)} items")
    if len(data) > 0:
        logger.debug(f"Item shape: {list(data[0].keys())}")

    # start item cycler on a separate threat
    cycler = Thread(target=cycleItems, args=[data, flags])
    cycler.start()

    uvicorn.run(app, port=flags.port)

    cycler.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", "-i", help="File (JSON) to read stream "
                        + "data from.", default="./dummy-data.json", type=str,
                        required=True)
    parser.add_argument("--cachesize", help="Cache size in number of "
                        + "publications (default 1)", default=1, type=int)
    parser.add_argument("--interval", help="Publishing interval in seconds "
                        + "(default 1:10). Takes a random value from the range"
                        + " 'from:to' (or shorthand ':to' when 'from' is 1) or"
                        + " 'int' if a fixed interval is desired.",
                        default=range(1, 10), type=integerRangeArg)
    parser.add_argument("--port", help="Bind socket to this port (default "
                        + "8000)", default=8000, type=int)
    parser.add_argument("--realtime", help="Use actual time of publication "
                        + " when publishing items.", default=False,
                        action="store_true")
    parser.add_argument("--timeout", help="Number of seconds until an open "
                        "connection timeouts.", default=30, type=int)
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

    main(flags)

#!/usr/bin/env python

"""
Dummy RESTful API for debugging purposes

Run as
 fastapi dev repo/gladoss/adaptors/dummy-device.py
"""

import argparse
import logging
import json
from time import sleep
from threading import Thread
from queue import Queue
from random import randrange

from fastapi import FastAPI
import uvicorn

app = FastAPI()
logger = logging.getLogger(__name__)


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/stream/")
def publish() -> dict[str, str]:
    global cache, item_prev
    if not cache.empty():
        item_cur = cache.get()  # dict[str, str]
        item_prev = item_cur
    else:
        item_cur = item_prev

    return item_cur


def cycleItems(data: list[dict[str, str]], flags: argparse.Namespace):
    """ Cycle through items, publishing each one after a random or fixed delay.
    Queue items if a cachesize > 1 is specified.

    :param data: list with items to publish
    :param flags: user-provided parameters
    """
    global cache
    for i, item in enumerate(data):
        logger.info(f"Publishing item {i+1} / {len(data)}: {item}")

        if not cache.full():
            cache.put(item)
        else:
            logger.debug(f"Cache is full")

        delay = getDelay(flags.interval)
        logger.debug(f"Waiting for {delay} seconds")
        sleep(delay)

    logger.info("No further publications")


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
    global cache, item_prev

    data = load_json(flags.input)  # list[dict[str, str]]
    item_prev = {"", ""}
    cache = Queue(maxsize=flags.cachesize)

    logger.info(f"Loaded {len(data)} items")
    if len(data) > 0:
        logger.debug(f"Item shape: {data[0].keys()}")

    # start item cycler on a separate threat
    cycler = Thread(target=cycleItems, args=[data, flags])
    cycler.start()

    uvicorn.run(app)

    cycler.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", "-i", help="File (JSON) to read stream "
                        + "data from.", default="./dummy-data.json", type=str,
                        required=True)
    parser.add_argument("--cachesize", help="Cache size in number of "
                        + "publications (default 1)", default=1, type=int)
    parser.add_argument("--verbose", "-v", help="Show debug messages in "
                        + "console.", action='count', default=0)
    parser.add_argument("--interval", help="Publishing interval in seconds "
                        + "(default 1:10). Takes a random value from the range"
                        + " 'from:to' (or shorthand ':to' when 'from' is 1) or"
                        + " 'int' if a fixed interval is desired.",
                        default=range(1, 10), type=integerRangeArg)

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

#! /usr/bin/env python

from enum import Enum, auto


class StreamInfoElement(Enum):
    RINTERVAL = auto()  # reception interval
    RTIME = auto()  # reception time

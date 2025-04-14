#!/usr/bin/env python

from argparse import Namespace
from datetime import timedelta
import re
from types import SimpleNamespace


def integerRangeArg(arg: str) -> range:
    """ Custom argument type for range

    :param arg: user provided argument string of form 'from:to', ':to', or
        'to', with 'from' and 'to' being positive integers.
    :type arg: str
    :rtype: range
    :returns: range of values to explore
    """

    begin = 0
    arg_lst = arg.split(':')
    try:
        end = int(arg_lst[-1])
        if len(arg_lst) > 1 and len(arg_lst[0]) > 0:
            begin = int(arg_lst[0])
    except Exception:
        raise Exception("'" + arg + "' is not a range of numbers. "
                        + "Expects input such as '0:3', ':3', or '3'.")

    # check if range is valid
    assert begin >= 0 and end >= 0 and begin <= end

    return range(begin, end)


def timeSpanArg(arg: str) -> timedelta:
    """ Custom argument type for time span

        Turns a string argument in a timedelta object. Expects the input to be
        an integer followed by 'H', 'D', or 'W', denoting hours, days, or
        weeks.

    :param arg: input argument
    :return: a corresponding timedelta object
    """
    re_pattern = r"(?P<value>[0-9]+)\s*(?P<unit>[dhwDHW])"

    value = -1
    unit = ''
    try:
        match = re.fullmatch(re_pattern, arg.strip())

        value = int(match.group('value'))
        unit = match.group('unit').upper()
    except Exception:
        raise Exception("'" + arg + "' is not a valid time span. "
                        + "Expects input such as '12H', '7D', or '4W'.")

    if unit == "H":
        delta = timedelta(hours=value)
    elif unit == "D":
        delta = timedelta(days=value)
    elif unit == "W":
        delta = timedelta(weeks=value)
    else:
        raise Exception()

    return delta


def create_namespace_subset(namespace: Namespace,
                            members: list) -> SimpleNamespace:
    """ Return a subset of the provided namespace which only
        holds the specified members (if present).

    :param namespace: [TODO:description]
    :param members: [TODO:description]
    :return: [TODO:description]
    """
    return SimpleNamespace(**{arg: getattr(namespace, arg, None)
                              for arg in members})

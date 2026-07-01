#!/usr/bin/env python

from argparse import Namespace
from datetime import timedelta
import logging
import re
import sys
import termios
import tty
from types import SimpleNamespace
from typing import Any, Callable, Optional

from rdf.terms import BNode, IRIRef


logger = logging.getLogger(__name__)


def getCh() -> str:
    """ Return character on key press. Blocks until event occurs.
    """
    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)
    try:
        tty.setraw(sys.stdin.fileno())
        ch = sys.stdin.read(1)

    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)

    return ch


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
    re_pattern = r"(?P<value>[0-9]+)\s*(?P<unit>[mdhwMDHW])"

    value = -1
    unit = ''
    try:
        match = re.fullmatch(re_pattern, arg.strip())

        value = int(match.group('value'))
        unit = match.group('unit').upper()
    except Exception:
        raise Exception("'" + arg + "' is not a valid time span. "
                        + "Expects input such as '12H', '7D', or '4W'.")

    if unit == "M":
        delta = timedelta(minutes=value)
    elif unit == "H":
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


def jsonpath_deref(root: dict | list, jsonpath: list[Any]) -> Any:
    """ Dereference a (nested) dictionary and/or list structure via
        a JSONpath. Only a subset of the JSONpath specification
        consisting of integer or string selectors in dot-notation is
        supported.

        Examples:
        - '$.my.data.[0].values.[5]'
        - '$.[0].[5].values'

    :param root: [TODO:description]
    :param jsonpath: [TODO:description]
    :return: [TODO:description]
    """
    pattern = r"([a-z0-9_-]+|(?<=\[)[0-9]+(?=\]))"

    def traverse(struc: Any, path: list[str | int]) -> Any:
        """ Traverse a (nested) dictionary or list recursively
            using the keys and/or indices in the path.

        :param struc: [TODO:description]
        :param path: [TODO:description]
        :return: [TODO:description]
        """
        if not (isinstance(struc, dict) or isinstance(struc, list)) \
           or len(path) <= 0:
            return struc

        key = path[0]
        if isinstance(struc, dict):
            val = struc.get(key, None)
        if isinstance(struc, list):
            try:
                val = struc[key]
            except (IndexError, TypeError):
                val = None

        return traverse(val, path[1:])

    path_lst = re.findall(pattern, jsonpath)
    for i in range(len(path_lst)):
        # cast strings of indices to integers
        try:
            path_lst[i] = int(path_lst[i])
        except ValueError:
            continue

    return traverse(root, path_lst)


def mknode(namespace: Optional[IRIRef], mkid: Callable) -> BNode | IRIRef:
    """ Create a new blank node or an IRI if a namespace is provided.

    :param namespace: [TODO:description]
    :param mkid: [TODO:description]
    :return: [TODO:description]
    """
    rand_id = mkid()

    return BNode(rand_id) if namespace is None else namespace + rand_id

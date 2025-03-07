#!/usr/bin/env python

import logging
import re
from typing import Self

from rdf import IRIRef, Literal, Statement

from gladoss.adaptors.adaptor import Adaptor

logger = logging.getLogger(__name__)

URI_CHARSET = r"[a-zA-Z0-9\-._~:/?#[\]@!$&'()*+,;=]"
URI = rf"<?{URI_CHARSET}+>?"
RESOURCE = rf"(?:{URI})|(?:'.*'(?:(?:@[a-z]{{2}})|(?:\^\^{URI}))?)"
STATEMENT = re.compile(rf"\((?P<head>{URI})"
                       rf"\s*(?P<relation>{URI})"
                       rf"\s*(?P<tail>{RESOURCE})\)")
LITERAL = re.compile(r"(?P<value>'.*')(?:"
                     r"(?:@(?P<lang>[a-z]{{2}}))|"
                     rf"(?:\^\^(?P<dtype>{URI})))?")


class DummyAdaptor(Adaptor):
    """ Adaptor to dummy device for debugging purposes

        Expects data in the form {"data": "(s p o) [...]"},
        with
        - s, p, o as 'ex:u' or '<http://www.example.org/u>'
        - or o as 'v', 'v@lang', or 'v^^dt'
        - and dt as 'ex:u' or '<http://www.example.org/u>'
    """

    def translate(self: Self, data: dict[str, str], **kwargs)\
            -> tuple[list[Statement], list[IRIRef]]:
        """ Translate dummy data to RDF.

        :param data: data received from API
        :return: A list of RDF statements and anchors
        :raises SyntaxWarning: warn if translation fails
        """
        graph = list()
        anchors = list()

        if "data" not in data.keys() or len(data["data"]) <= 0:
            logging.debug("Missing content in data package")
            return (graph, anchors)

        graph_str = data["data"]
        try:
            for match in re.finditer(STATEMENT, graph_str):
                logging.debug(match.groupdict())
                fact = self.process_fact(match)
                graph.append(fact)
        except Exception:
            raise SyntaxWarning(f"Unexpected data format: {graph_str}")

        anchors.append([IRIRef('o')])

        return (graph, anchors)

    def process_fact(self: Self, match: re.Match) -> Statement:
        """ Process single string-encoded fact and return a RDF statement.

        :param match: A matching regex object
        :return: The corresponding RDF statement
        """
        head = IRIRef(match.group('head'))
        relation = IRIRef(match.group('relation'))

        tail = match.group('tail')  # type: str
        tail_literal = re.fullmatch(LITERAL, tail)
        if tail_literal:
            value = tail_literal.group('value')
            lang = tail_literal.group('lang')
            dtype = tail_literal.group('dtype')

            if dtype:
                dtype = IRIRef(dtype)

            tail = Literal(value, datatype=dtype, language=lang)

        return Statement(head, relation, tail)

#!/usr/bin/env python

import logging
import re
from sys import stdout
from typing import Any, Collection, Self

from gladoss.core.connector import Connector
from rdf import IRIRef, Literal, Statement

from gladoss.adaptors.adaptor import Adaptor

logger = logging.getLogger(__name__)

URI_CHARSET = r"[a-zA-Z0-9\-._~:/?#[\]@!$&'()*+,;=]"
URI = rf"<?{URI_CHARSET}+>?"
RESOURCE = rf"(?:{URI})|(?:\".*\"(?:(?:@[a-z]{{2}})|(?:\^\^{URI}))?)"
STATEMENT = re.compile(rf"(?P<head>{URI})"
                       rf"\s*(?P<relation>{URI})"
                       rf"\s*(?P<tail>{RESOURCE})\s*\.")
LITERAL = re.compile(r"\"(?P<value>.*)\"(?:"
                     r"(?:@(?P<lang>[a-z]{{2}}))|"
                     rf"(?:\^\^(?P<dtype>{URI})))?")


class DummyAdaptor(Adaptor):
    """ Adaptor to dummy device for debugging and demo purposes.

        Expects data in the form {"label": <STRING>,
                                  "data": "s p o . [...]"},
        with
        - s, p, o as '<http://www.example.org/u>'
        - or o as '"v"', '"v"@lang', or '"v"^^dt'
        - and dt as '<http://www.example.org/u>'
        - and lang as [a-z]{2}

        Publishes data to standard output in the form "s p o . [...]",
        with
        - s, p, o as '<http://www.example.org/u>'
        - or o as '"v"', '"v"@lang', or '"v"^^dt'
        - and dt as or '<http://www.example.org/u>'
        - and lang as [a-z]{2}
    """

    def init_hook(self: Self) -> None:
        """ Execute additional commands on initialisation.
        """
        return super().init_hook()

    def cleanup_hook(self: Self) -> None:
        """ Execute additional commands on exit.
        """
        return super().cleanup_hook()

    def set_headers(self: Self) -> dict[str, Any]:
        """ Returns headers for polling the endpoint. Defaults
            to empty headers

        :param self: [TODO:description]
        :return: [TODO:description]
        """
        return {"adaptor": "dummy"}

    def set_payload(self: Self) -> dict[str, Any]:
        """ Returns payload for polling the endpoint. Defaults
            to empty payload.

        :param self: [TODO:description]
        :return: [TODO:description]
        """

        return super().set_payload()

    def set_receipt_headers(self: Self, data: dict[str, Any])\
            -> dict[str, Any]:
        """ Returns headers for sending a receipt. Defaults
            to empty headers.

        :param self: [TODO:description]
        :return: [TODO:description]
        """

        return super().set_receipt_headers(data)

    def set_receipt_payload(self: Self, data: dict[str, Any])\
            -> dict[str, Any]:
        """ Returns payload for sending a receipt. Defaults
            to empty payload.

        :param self: [TODO:description]
        :return: [TODO:description]
        """

        return super().set_receipt_payload(data)

    def add_connectors(self: Self) -> None:
        """ Add connectors to adaptor, one for each different endpoint
            or request type.
        """
        self.connectors.add(Connector(
            adaptor=self,
            endpoint=self.config.endpoint,
            continuous=self.config.continuous,
            num_retries=self.config.retries,
            retry_delay=self.config.retry_delay,
            request_delay=self.config.request_delay,
            return_receipt=self.config.return_receipt
            ))

    def publish_report(self: Self, identifier: str,
                       data: Collection[Statement]) -> bool:
        """ Write the validation report (as N-Triples) for
            the state graph with the provided identifier to
            the standard output

        :param identifier: [TODO:description]
        :param data: [TODO:description]
        :return: [TODO:description]
        """
        stdout.write("--- BEGIN Validation Report %s ---\n" % identifier)
        for assertion in data:
            stdout.write(" %s\n" % str(assertion))
        stdout.write("--- END Validation Report %s ---\n" % identifier)

        return True

    def translate(self: Self, data: dict[str, Any])\
            -> list[tuple[str, list[Statement]]]:
        """ Translate dummy data to RDF.

        :param data: data received from API
        :return: A list of RDF statements and their identifier
        :raises SyntaxWarning: warn if translation fails
        """
        data_translated = list()
        if "data" not in data.keys() or len(data["data"]) <= 0:
            logging.debug("Missing content in data package")
            return data_translated

        if "label" not in data.keys():
            logging.debug("Missing graph identifier in data package")
            return data_translated

        graph_id = data['label']  # type: str
        graph_str = data['data'].strip()  # tyoe: str
        try:
            graph = list()
            for match in re.finditer(STATEMENT, graph_str):
                logging.debug(match.groupdict())
                fact = self.process_fact(match)
                graph.append(fact)

            data_translated.append((graph_id, graph))
        except Exception:
            raise SyntaxWarning(f"Unexpected data format: {graph_str}")

        return data_translated

    def process_fact(self: Self, match: re.Match) -> Statement:
        """ Process single string-encoded fact and return a RDF statement.

        :param match: A matching regex object
        :return: The corresponding RDF statement
        """
        head = IRIRef(self.process_IRI(match.group('head')))
        relation = IRIRef(self.process_IRI(match.group('relation')))

        tail = match.group('tail')  # type: str
        tail_literal = re.fullmatch(LITERAL, tail)
        if tail_literal:
            value = tail_literal.group('value')
            lang = tail_literal.group('lang')
            dtype = tail_literal.group('dtype')

            if dtype:
                dtype = IRIRef(self.process_IRI(dtype))

            tail = Literal(value, datatype=dtype, language=lang)
        else:
            tail = IRIRef(self.process_IRI(match.group('tail')))

        return Statement(head, relation, tail)

    def process_IRI(self: Self, resource: str) -> IRIRef:
        if resource.startswith("<") and resource.endswith(">"):
            resource = resource[1:-1]

        return resource

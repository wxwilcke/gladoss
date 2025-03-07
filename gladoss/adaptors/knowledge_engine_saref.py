#!/usr/bin/env python

import logging
import re
from typing import Any, Self

import requests
from rdf import IRIRef, Literal, Statement

from gladoss.adaptors.adaptor import Adaptor

logger = logging.getLogger(__name__)


URI_CHARSET = r"[a-zA-Z0-9\-._~:/?#[\]@!$&'()*+,;=]"
URI = rf"<?{URI_CHARSET}+>?"
RESOURCE = rf"(?:{URI})|(?:'.*'(?:(?:@[a-z]{{2}})|(?:\^\^{URI}))?)"
STATEMENT = re.compile(rf"(?P<head>{URI})"
                       rf"\s*(?P<relation>{URI})"
                       rf"\s*(?P<tail>{RESOURCE})\s*\.")
LITERAL = re.compile(r"(?P<value>'.*')(?:"
                     r"(?:@(?P<lang>[a-z]{{2}}))|"
                     rf"(?:\^\^(?P<dtype>{URI})))?")


# TODO: load from config file
KE_PATTERN = """ ?obs rdf:type saref:Observation .
                 ?obs saref:madeBy ?device . 
                 ?obs saref:hasTimestamp ?timestamp . 
                 ?obs saref:hasResult ?result .
                 ?result rdf:type saref:PropertyValue .
                 ?result saref:isValueOfProperty ?prop .
                 ?result saref:hasValue ?value .
                 ?result saref:isMeasuredIn ?unit .
             """
KE_PREFIX = { "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
              "saref": "https://saref.etsi.org/core/" }
ANCHORS = ["device", "prop"]


class KESarefAdaptor(Adaptor):
    """ Adaptor to TNO's Knowledge Engine

        Expects data in the form {"bindingSet": [{ "u": "<IRI>|<Literal>",
                                                   "v": "<IRI>|<Literal>",
                                                   ...},
                                                  ...]
                                 }
        with
        - "u", "v" the variable name, matching "?<var>"
        - { ... } a single measurement from a specific device
        - [ ... ] a list of measurements from multiple devices in (1, inf)
    """

    def init_hook(self: Self) -> None:
        # TODO: load from config file
        kb_id = "http://example.org/ai"
        kb_name = "Anomaly Detector"
        kb_description = "An anomaly dectector for streaming graph data"

        try:
            self.register(kb_id, kb_name, kb_description)

            ki_name = "Measurements"
            self.subscribe(KE_PATTERN, ki_name, kb_id, KE_PREFIX)
        except Exception:
            pass

    def register(self: Self, kb_id: str, kb_name: str,
                 kb_description: str):
        """
        Register a Knowledge Base with the given details at the given endpoint.
        """
        body = {"knowledgeBaseId": kb_id,
                "knowledgeBaseName": kb_name,
                "knowledgeBaseDescription": kb_description}

        response = requests.post(self.endpoint, json=body)
        assert response.ok

        logger.info(f"registered {kb_name}")

    def subscribe(self: Self, triple_pattern: str, ki_name: str,
                  kb_id: str, prefixes: dict[str, str]) -> str:
        body = {"knowledgeInteractionName": ki_name,
                "knowledgeInteractionType": "AskKnowledgeInteraction",
                "graphPattern": triple_pattern,
                "prefixes": prefixes}

        response = requests.post(self.endpoint, json=body,
                                 headers={"Knowledge-Base-Id": kb_id})
        assert response.ok

        ki_id = response.json()["knowledgeInteractionId"]
        logger.info(f"received issued knowledge interaction id: {ki_id}")

        return ki_id


    def translate(self: Self, data: dict[str, Any], **kwargs)\
            -> tuple[list[Statement], list[IRIRef]]:
        """ Translate binding sets to RDF.

        :param data: data received from API
        :return: A list of RDF statements and anchors
        :raises SyntaxWarning: warn if translation fails
        """
        graph = list()
        anchors = list()

        if "bindingSet" not in data.keys() or len(data["bindingSet"]) <= 0:
            logging.debug("Missing content in data package")
            return (graph, anchors)

        bindings = data["bindingSet"]  # type: list[dict[str,str]]
        try:
            statements_str = [s.strip() for s in KE_PATTERN.split('\n')]
            for bset in bindings:
                for s in self.instantiate_graph(statements_str, bset):
                    match = re.fullmatch(STATEMENT, s)
                    if match is not None:
                        fact = self.process_fact(match)

                        graph.append(fact)

                # TODO: make URIs
                anchors = [v for v in bset if v in bset.keys()]
        except Exception:
            raise SyntaxWarning(f"Unexpected data format: {bindings}")


        return (graph, anchors)  # TODO: change to yield?

    def instantiate_graph(self: Self, statements_str: list[str],
                          bindings: dict[str, str]) -> list[str]:

        out = list()
        for s in statements_str:
            if len(s) <= 0:
                continue

            i = 0
            for var, vbind in bindings.items():
                if f"?{var}" in s:
                    # TODO: check if literals are correctly encased in quotes
                    s = s.replace(f"?{var}", vbind)
                    i += 1

                if i >= 3:
                    # at most three bindings per statement
                    break

            out.append(s)

        return out

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

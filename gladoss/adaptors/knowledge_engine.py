#!/usr/bin/env python

import logging
import os
import re
import toml
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

FILE_DIR = os.path.dirname(__file__)
FILENAME_CONF = "knowledge_engine.toml"
CONF_PATH = os.path.join(FILE_DIR, FILENAME_CONF)


# TODO:
# - accomodate multiple subscriptions

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
        conf = dict()
        with open(CONF_PATH, 'rb') as f:
            conf = toml.load(f)

        self.context = dict()

        kb_id = conf['knowledgeBaseId']
        kb_name = conf['knowledgeBaseName']
        kb_description = conf['knowledgeBaseDescription']
        try:
            self.context['knowledgeBaseId'] = kb_id
            self.register(kb_id, kb_name, kb_description)

            self.context['knowledgeInteractions'] = list()
            for ki in conf['knowledgeInteraction']:
                self.context['knowledgeInteractions'].append(ki)
                
                # ki_anchors = ki['knowledgeInteractionAnchors']

        except Exception:
            pass

    def set_headers(self: Self, **kwargs) -> dict[str, Any]:
        kb_id = self.context['knowledgeBaseId']

        return {"Knowledge-Base-Id": kb_id}

    def set_payload(self: Self, **kwargs) -> dict[str, Any]:
        ki = self.context['knowledgeInteractions'][i]

        ki_name = ki['knowledgeInteractionName']
        ki_type = ki['knowledgeInteractionType']
        ki_pattern = ki['knowledgeInteractionPattern']
        ki_prefixes = ki['prefixes']

        body = {"knowledgeInteractionName": ki_name,
                "knowledgeInteractionType": ki_type,
                "graphPattern": ki_pattern,
                "prefixes": ki_prefixes}

        return body

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

    def subscribe(self: Self, kb_id: str, ki_name: str, ki_type: str,
                  triple_pattern: str, prefixes: dict[str, str]) -> str:
        body = {"knowledgeInteractionName": ki_name,
                "knowledgeInteractionType": ki_type,
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
            statements_str = [s.strip() for s in KE_PATTERN.splitlines()]
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
        """ Replace variable names by bounded values. This will fail
            if a variable name occurs inside a string literal, but
            this is very unlikely.

        :param statements_str: the triple pattern as strings
        :param bindings: a map between variable names and their values
        :return: the instantiated triples as strings
        """

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

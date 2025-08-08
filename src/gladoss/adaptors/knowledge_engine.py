#!/usr/bin/env python

import logging
import os
import re
import toml
from typing import Any, Collection, Optional, Self

import requests
from rdf import IRIRef, Literal, Statement

from gladoss.adaptors.adaptor import Adaptor
from gladoss.core.connector import Connector

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
REPORT_GRAPH_PATTERN = """
?root rdf:type sh:ValidationReport .
?root dct:date ?date .
?root dct:identifier ?identifier .
?root dct:conformsTo ?specification .
?root sh:conforms ?conforms .
?root dct:hasPart ?result .

?result rdf:type sh:ValidationResult .
?result rdfs:label ?status_msg .
?result sh:focusNode ?focusNode .
?result sh:resultPath ?resultPath .
?result sh:value ?value .
?result sh:sourceShape ?sourceShape .
?result sh:resultMessage ?status_msg_long .
?result sh:resultSeverity ?severity .

?severity rdf:type sh:Severity .
?severity rdfs:label ?status_code .
?severity rdfs:comment ?status_description .
"""
REPORT_PREFIXES = {
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
        "dct": "http://purl.org/dc/terms/",
        "sh": "http://www.w3.org/ns/shacl#"
        }


class KE_Adaptor(Adaptor):
    """ Adaptor to TNO's Knowledge Engine

        Expects data in the form {"requestingKnowledgeBaseId": <INT>,
                                  "knowledgeInteractionId": <INT>,
                                  "handleRequestId": <INT>,
                                  "bindingSet": [{ "u": "<IRI>|<Literal>",
                                                   "v": "<IRI>|<Literal>",
                                                   ...},
                                                  ...]
                                 }
        with
        - "u", "v" the variable name, matching "?<var>"
        - { ... } a binding set between the variables and their values
        - [ ... ] a list of all possible binding sets for this observation
    """

    def register_kb(self: Self, endpoint: str, kb_id: str, kb_name: str,
                    kb_desc: str) -> Optional[dict[str, Any]]:
        """ Register knowledge base if it has not been registered yet.

        :param endpoint: [TODO:description]
        :param kb_id: [TODO:description]
        :param kb_name: [TODO:description]
        :param kb_desc: [TODO:description]
        """
        endpoint = endpoint + "/sc"
        headers = {'Knowledge-Base-Id': kb_id}

        response = requests.get(endpoint, headers=headers)
        if response.status_code == requests.codes.not_found:  # 404: not found
            payload = {'knowledgeBaseId': kb_id,
                       'knowledgeBaseName': kb_name,
                       'knowledgeBaseDescription': kb_desc}
            response = requests.post(endpoint, json=payload)

        if response.status_code == requests.codes.ok:  # 200: ok
            smartconnectors = response.json()  # type: list
            assert len(smartconnectors) == 1, "Expects exactly one SC"
            return smartconnectors.pop()  # type: dict[str, Any]

    def register_ki(self: Self, endpoint: str, kb_id: str,
                    ki_payload: dict[str, str]) -> Optional[str]:
        """ Register knowledge interaction.

        :param endpoint: [TODO:description]
        :param kb_id: [TODO:description]
        :param ki_payload: [TODO:description]
        """
        endpoint = endpoint + "/sc/ki"
        headers = {'Knowledge-Base-Id': kb_id}

        response = requests.post(endpoint, headers=headers, json=ki_payload)
        if response.status_code == requests.codes.ok:  # 200: ok
            return response.json()['KnowledgeInteractionId']  # type: str

    def deregister_kb(self: Self, endpoint: str, kb_id: str):
        """ Deregister knowledge base.

        :param endpoint: [TODO:description]
        :param kb_id: [TODO:description]
        """
        endpoint = endpoint + "/sc"
        headers = {'Knowledge-Base-Id': kb_id}

        response = requests.delete(endpoint, headers=headers)

        return response.status_code == requests.codes.ok  # 200: ok

    def deregister_ki(self: Self, endpoint: str, kb_id: str, ki_id: str):
        """ Deregister knowledge interaction.

        :param endpoint: [TODO:description]
        :param kb_id: [TODO:description]
        :param ki_id: [TODO:description]
        """
        endpoint = endpoint + "/sc/ki"
        headers = {'Knowledge-Base-Id': kb_id,
                   'Knowledge-Interaction-Id': ki_id}

        response = requests.delete(endpoint, headers=headers)

        return response.status_code == requests.codes.ok  # 200: ok

    def post_ki(self: Self, endpoint: str, ki_headers: dict[str, Any],
                ki_payload: dict[str, Any]) -> bool:
        """ Perform a post knowledge interaction.

        :param endpoint: [TODO:description]
        :param ki_headers: [TODO:description]
        :param ki_payload: [TODO:description]
        """
        endpoint = endpoint + "/sc/post"
        response = requests.post(endpoint, headers=ki_headers, json=ki_payload)

        return response.status_code == requests.codes.ok  # 200: ok

    def init_hook(self: Self) -> None:
        """ Register the knowledge base and knowledge interactions. The
            knowledge interactions should be of the type 'react', and
            will have an empty results graph.
        """
        conf = dict()
        with open(CONF_PATH, 'rb') as f:
            conf = toml.load(f)

        # use context to share data between hooks
        self.context['reactKnowledgeInteractions'] = dict()
        self.context['reactKnowledgeInteractionsInv'] = dict()
        self.context['postKnowledgeInteractions'] = dict()
        self.context['postKowledgeInteractionConnectors'] = dict()
        self.context['argumentGraphPatterns'] = dict()

        kb_id = conf['knowledgeBaseId']
        kb_name = conf['knowledgeBaseName']
        kb_desc = conf['knowledgeBaseDescription']
        for ki in conf['knowledgeInteractions']:  # register all interactions
            ki_endpoint = ki['knowledgeInteractionEndpoint']
            if ki_endpoint not in self.context['reactKnowledgeInteractions']:
                # keep track of registered knowledge interactions
                self.context['reactKnowledgeInteractions'][ki_endpoint] = set()

            ki_pattern = ki['argumentGraphPattern']
            ki_payload = {
                    'knowledgeInteractionType': "ReactKnowledgeInteraction",
                    'knowledgeInteractionName': ki['knowledgeInteractionName'],
                    'argumentGraphPattern': ki_pattern,
                    'resultGraphPattern': "",  # empty response
                    'prefixes': ki['prefixes']
                    }
            try:
                sc = self.register_kb(ki_endpoint, kb_id, kb_name, kb_desc)
                if sc is None:
                    raise Exception("Unable to register knowledge base")

                ki_id = self.register_ki(ki_endpoint, kb_id, ki_payload)
                if ki_id is None:
                    logger.error("Unable to register knowledge interaction")

                    continue

                self.context['reactKnowledge'
                             'Interactions'][ki_endpoint].add(ki_id)
                self.context['reactKnowledge'
                             'InteractionsInv'][ki_id] = ki_endpoint
                self.context['argumentGraphPatterns'][ki_id] = ki_pattern
            except Exception as e:
                logger.error(f"Unable to register at endpoint: {e}.")

        self.context['knowledgeBaseId'] = kb_id

        # necessary for knowledge engine
        self.config.return_receipt = True

        # register KIs for report publication
        self.register_report_publications()

    def register_report_publications(self: Self) -> None:
        """ Register a single post knowledge interaction per known
            endpoint to send validation reports to. The reports are
            send to the same endpoint as where the graph, about which
            the report reports, are received from.
        """
        kb_id = self.context['knowledgeBaseId']
        for ki_endpoint in self.context['reactKnowledgeInteractions'].keys():
            ki_payload = {
                    'knowledgeInteractionType': "PostKnowledgeInteraction",
                    'knowledgeInteractionName': "AnomalyReportPublication",
                    'argumentGraphPattern': REPORT_GRAPH_PATTERN,
                    'resultGraphPattern': "",  # empty response
                    'prefixes': REPORT_PREFIXES
                    }

            ki_id = self.register_ki(ki_endpoint, kb_id, ki_payload)

            # keep track of registered knowledge interactions
            self.context['postKnowledgeInteractions'][ki_endpoint] = ki_id

    def cleanup_hook(self: Self):
        """ Deregister the knowledge base and all associated knowledge
            interactions.
        """
        kb_id = self.context['knowledgeBaseId']
        for ki_endpoint, ki_set in self.context['reactKnowledgeInteractions']:
            endpoint = ki_endpoint + "/sc/ki"
            for ki_id in ki_set:
                self.deregister_ki(endpoint, kb_id, ki_id)

            self.deregister_kb(endpoint, kb_id)

    def publish_report(self: Self, identifier: str,
                       data: Collection[Statement]) -> bool:
        """ Publish the validation report (as N-Triples) for
            the state graph with the provided identifier, by
            performing a post knowledge interaction to the
            endpoint from which the state graph was received.

        :param identifier: [TODO:description]
        :param data: [TODO:description]
        :return: [TODO:description]
        """
        success = False
        try:
            package_headers = self.set_report_headers(identifier)
            package_payload = self.set_report_payload(identifier, data)

            ki_endpoint = self.context['reactKnowledge'
                                       'InteractionsInv'][identifier]

            success = self.post_ki(ki_endpoint,
                                   package_headers,
                                   package_payload)
        except KeyError as e:
            logger.error(f"Unable to publish report to endpoint: {e}.")

        return success

    def add_connectors(self: Self) -> None:
        """ Add connectors to adaptor, one for each different endpoint.
        """
        # connections to listen on
        for ki_endpoint in self.context['reactKowledgeInteractions'].keys():
            self.connectors.add(Connector(
                adaptor=self,
                endpoint=ki_endpoint + "/sc/handle",
                continuous=self.config.continues,
                num_retries=self.config.retries,
                retry_delay=self.config.retry_delay,
                request_delay=self.config.request_delay,
                return_receipt=self.config.return_receipt
                ))

    def set_headers(self: Self) -> dict[str, Any]:
        """ Returns headers for polling the endpoint. Defaults
            to empty headers

        :param self: [TODO:description]
        :return: [TODO:description]
        """
        kb_id = self.context['knowledgeBaseId']

        return {"Knowledge-Base-Id": kb_id}

    def set_payload(self: Self) -> dict[str, Any]:
        """ Returns payload for polling the endpoint. Defaults
            to empty payload.

        :param self: [TODO:description]
        :return: [TODO:description]
        """
        return super().set_payload()

    def set_report_headers(self: Self, identifier: str) -> dict[str, Any]:
        """ Returns headers for publishing the validation report
            to the endpoint. Defaults to empty headers

        :param self: [TODO:description]
        :return: [TODO:description]
        """
        headers = dict()
        try:
            ki_endpoint = self.context['reactKnowledge'
                                       'InteractionsInv'][identifier]
            ki_id = self.context['postKnowledgeInteractions'][ki_endpoint]

            headers = {
                    'Knowledge-Base-Id': self.context['knowledgeBaseId'],
                    'Knowledge-Interaction-Id': ki_id
                    }
        except KeyError:
            logger.error("Unable to retrieve identifiers for report "
                         "publication headers.")

        return headers

    def set_report_payload(self: Self, identifier: str,
                           data: Collection[Statement]) -> dict[str, Any]:
        """ Returns payload for publishing the validation report
            to the endpoint, by converting the provided shape graph
            to binding sets.

        :param self: [TODO:description]
        :return: [TODO:description]
        """

        return self.translate_inv(data)  # TODO

    def set_receipt_headers(self: Self, data: dict[str, Any])\
            -> dict[str, Any]:
        """ Returns headers for sending a receipt. Defaults
            to empty headers.

        :param self: [TODO:description]
        :return: [TODO:description]
        """
        headers = dict()
        try:
            headers = {
                    'Knowledge-Base-Id': data['requestingKnowledgeBaseId'],
                    'Knowledge-Interaction-Id': data['knowledgeInteractionId']
                    }
        except KeyError:
            logger.error("Unable to retrieve identifiers from message "
                         + "payload.")

        return headers

    def set_receipt_payload(self: Self, data: dict[str, Any])\
            -> dict[str, Any]:
        """ Returns payload for sending a receipt. Defaults
            to empty payload.

        :param self: [TODO:description]
        :return: [TODO:description]
        """
        payload = dict()
        try:
            req_id = data['handleRequestId']
            payload = {
                    'handleRequestId': req_id,
                    'bindingSet': list()  # empty response
                    }
        except KeyError:
            logger.error("Unable to retrieve identifiers from message "
                         + "payload.")

        return payload

    def translate(self: Self, data: dict[str, Any])\
            -> list[tuple[str, list[Statement]]]:
        """ Translate binding sets to RDF.

        :param data: data received from API
        :return: A list of RDF statements and anchors
        :raises SyntaxWarning: warn if translation fails
        """

        data_translated = list()
        if "bindingSet" not in data.keys() or len(data["bindingSet"]) <= 0:
            logging.debug("Missing content in data package")
            return data_translated

        if "knowledgeInteractionId" not in data.keys():
            logging.debug("Missing graph identifier in data package")
            return data_translated

        ki_id = data['knowledgeInteractionId']  # type: str
        if ki_id not in self.context['argumentGraphPatterns'].keys():
            logging.debug("Unable to find argument graph pattern associated "
                          + f"with knowledge interaction {ki_id}")
            return data_translated

        ki_pattern = self.context['argumentGraphPatterns'][ki_id]  # type: str
        bindings = data["bindingSet"]  # type: list[dict[str,str]]
        try:
            statements_str = [s.strip() for s in ki_pattern.splitlines()]
            for bset in bindings:
                graph = list()
                for s in self.instantiate_graph(statements_str, bset):
                    match = re.fullmatch(STATEMENT, s)
                    if match is not None:
                        fact = self.process_fact(match)

                        graph.append(fact)

                data_translated.append((ki_id, graph))
        except Exception:
            raise SyntaxWarning(f"Unexpected data format: {bindings}")

        return data_translated

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

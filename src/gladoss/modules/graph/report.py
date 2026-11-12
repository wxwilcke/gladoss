#!/usr/bin/env python

from __future__ import annotations

import logging
from collections.abc import Collection
from datetime import datetime

from rdf.graph import Statement
from rdf.namespaces import RDF, RDFS, SHACL, XSD
from rdf.terms import IRIRef, Literal

from gladoss.core.report import ValidationReport
from gladoss.data.utils import mknode, string_to_node
from gladoss.modules.graph.pattern import GraphPattern

logger = logging.getLogger(__name__)

DCT = IRIRef("http://purl.org/dc/terms/")


class GraphValidationReport(ValidationReport):
    def __init__(
            self,
            node_id: str,
            stream_id: str,
            graph_id: str,
            pattern: GraphPattern,
            graph: Collection[Statement],
            apa_map: dict[str, Statement],
            timestamp: datetime,
            status_code: ValidationReport.StatusCode,
            status_msg_lst: list[tuple[str,
                                       str,
                                       ValidationReport.StatusCode,
                                       ValidationReport.ViolationType
                                       ]
                                 ] = [],
            status_msg_lst_map: dict[str,
                                     list[tuple[str,
                                                str,
                                                ValidationReport.StatusCode,
                                                ValidationReport.ViolationType
                                                ]
                                          ]
                                     ] = {}):
        """ A validation report for an obversed state graph with its associated
            pattern, status code, and description of the evaluation results.

        :param pattern: [TODO:description]
        :param graph: [TODO:descriptions
        :param timestamp: [TODO:description]
        :param status_code: [TODO:description]
        :param status_msg: [TODO:description]
        :param status_msg_long: [TODO:description]
        """
        super().__init__(
                type=ValidationReport.ReportType.GRAPH_VALIDATION_REPORT,
                node_id=node_id,
                stream_id=stream_id,
                graph_id=graph_id,
                timestamp=timestamp,
                status_code=status_code,
                status_msg_lst=status_msg_lst,
                status_msg_lst_map=status_msg_lst_map)

        self.pattern = pattern
        self.graph = graph
        self.apa_map = apa_map

    def to_graph(self, namespace: str | None)\
            -> list[Statement]:
        """ Convert a graph validation report object to RDF graph in N-Triples
            format that conforms to the SHACL specification. Each detected
            anomaly (or error) is converted to a SHACL validation result with
            information about the causing assertion and with a detailed
            explanation. Some metadata is added to the head of the graph.

            The output is of the following form:

            > ?report rdf:type sh:ValidationReport .
            > ?report dct:issued ?reportDate .
            > ?report dct:subject ?reportSubject .
            > ?report dct:conformsTo ?reportLanguage .
            > ?report sh:conforms ?validationPassed .
            > ?report dct:hasPart ?result .

            > ?result rdf:type sh:ValidationResult .
            > ?result rdfs:label ?resultStatusMsg .
            > ?result dct:type ?resultViolationType .
            > ?result sh:focusNode ?resultFocusNode .
            > ?result sh:resultPath ?resultPath .
            > ?result sh:value ?resultValue .
            > ?result sh:resultMessage ?resultStatusMsgLong .
            > ?result sh:resultSeverity ?resultSeverity .

            > ?resultSeverity rdf:type sh:Severity .
            > ?resultSeverity rdfs:label ?severityLabel .
            > ?resultSeverity rdfs:comment ?severityDescription .

        :param report: [TODO:description]
        :param mkid: [TODO:description]
        :return: [TODO:description]
        """
        logger.debug("Exporting graph validation report to SHACL "
                     f"(GID '{self.graph_id}')")

        # use provided namespace for new nodes
        if isinstance(namespace, str) and len(namespace) > 0:
            if not namespace.endswith(('/', '#')):
                namespace += '#'

            namespace = IRIRef(namespace)

        # default value
        conforms = True

        # define graph and metadata
        root = mknode(namespace)
        graph = [
            Statement(root, RDF + 'type', SHACL + 'ValidationReport'),
            Statement(root, DCT + 'issued',
                      Literal(self.timestamp.isoformat(),
                              datatype=XSD + 'dateTime')),
            Statement(root, DCT + 'subject', string_to_node(self.node_id)),
            Statement(root, DCT + 'conformsTo', Literal(
                "https://www.w3.org/TR/shacl/", datatype=XSD + 'anyURI'))
            ]

        # process violations and errors without associated shape
        for status_msg_lst in self.status_msg_lst:
            status_msg, status_msg_long, status_code, violation_type\
                    = status_msg_lst

            res = mknode(namespace)
            graph.extend([
                Statement(root, DCT + 'hasPart', res),
                Statement(res, RDF + 'type', SHACL + 'ValidationResult'),
                Statement(res, RDFS + 'label',
                          Literal(status_msg, language="en")),
                Statement(res, DCT + 'type', Literal(violation_type.name)),
                Statement(res, SHACL + 'resultMessage',
                          Literal(status_msg_long, language="en"))
                ])

            sev = mknode(namespace)
            graph.extend([
                Statement(res, SHACL + 'resultSeverity', sev),
                Statement(sev, RDF + 'type', SHACL + 'Severity'),
                Statement(sev, RDFS + 'label',
                          Literal(status_code.name, datatype=XSD + 'string')),
                Statement(sev, RDFS + 'comment',
                          Literal(status_code.description, language="en"))
                ])

            conforms = False

        # process violations and errors with associated shape
        # iterate over all validated shapes
        for ap_id, status_msg_lst in self.status_msg_lst_map.items():
            if ap_id not in self.apa_map:
                continue

            # one result per anomaly
            assertion = self.apa_map[ap_id]  # type: Statement
            for status_msg, status_msg_long, status_code, violation_type\
                    in status_msg_lst:
                res = mknode(namespace)
                graph.extend([
                    Statement(root, DCT + 'hasPart', res),
                    Statement(res, RDF + 'type', SHACL + 'ValidationResult'),
                    Statement(res, SHACL + 'focusNode', assertion.subject),
                    Statement(res, SHACL + 'resultPath', assertion.predicate),
                    Statement(res, SHACL + 'value', assertion.object),
                    Statement(res, RDFS + 'label',
                              Literal(status_msg, language="en")),
                    Statement(res, DCT + 'type', Literal(violation_type.name)),
                    Statement(res, SHACL + 'resultMessage',
                              Literal(status_msg_long, language="en"))
                    ])

                sev = mknode(namespace)
                graph.extend([
                    Statement(res, SHACL + 'resultSeverity', sev),
                    Statement(sev, RDF + 'type', SHACL + 'Severity'),
                    Statement(sev, RDFS + 'label',
                              Literal(status_code.name,
                                      datatype=XSD + 'string')),
                    Statement(sev, RDFS + 'comment',
                              Literal(status_code.description, language="en"))
                    ])

            conforms = False

        # summary of report - defaults to true if no anomalies have been found
        conforms = str(conforms).lower()
        graph.append(Statement(root, SHACL + 'conforms',
                               Literal(conforms, datatype=XSD + 'boolean')))

        return graph

    def __hash__(self):
        return hash(str(self.pattern)
                    + str(self.node_id)
                    + str(self.stream_id)
                    + str(self.graph)
                    + str(self.timestamp))

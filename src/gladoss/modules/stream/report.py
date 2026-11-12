#!/usr/bin/env python

from __future__ import annotations

import logging
from datetime import datetime

from rdf.graph import Statement
from rdf.namespaces import RDF, RDFS, SHACL, XSD
from rdf.terms import IRIRef, Literal

from gladoss.core.report import ValidationReport
from gladoss.data.utils import mknode, string_to_node

logger = logging.getLogger(__name__)

DCT = IRIRef("http://purl.org/dc/terms/")


class StreamValidationReport(ValidationReport):
    def __init__(
            self,
            node_id: str,
            stream_id: str,
            endpoint: str,
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
                                     ] | None = {}):
        """ A validation report for a transmission stream with its associated
            node, status code, and description of the evaluation results.

        :param pattern: [TODO:description]
        :param graph: [TODO:descriptions
        :param timestamp: [TODO:description]
        :param status_code: [TODO:description]
        :param status_msg: [TODO:description]
        :param status_msg_long: [TODO:description]
        """

        super().__init__(
                type=ValidationReport.ReportType.STREAM_VALIDATION_REPORT,
                node_id=node_id,
                stream_id=stream_id,
                graph_id=None,
                timestamp=timestamp,
                status_code=status_code,
                status_msg_lst=status_msg_lst,
                status_msg_lst_map=status_msg_lst_map)

        self.endpoint = endpoint

    def to_graph(self, namespace: str | None)\
            -> list[Statement]:
        """ Convert a stream validation report object to RDF graph in N-Triples
            format that conforms to the SHACL specification. Each detected
            anomaly (or error) is converted to a SHACL validation result with
            information with a detailed explanation. Some metadata is added
            to the head of the graph.

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
            > ?result sh:resultMessage ?resultStatusMsgLong .
            > ?result sh:resultSeverity ?resultSeverity .

            > ?resultSeverity rdf:type sh:Severity .
            > ?resultSeverity rdfs:label ?severityLabel .
            > ?resultSeverity rdfs:comment ?severityDescription .

        :param report: [TODO:description]
        :param mkid: [TODO:description]
        :return: [TODO:description]
        """
        logger.debug("Exporting stream validation report to SHACL "
                     f"(SID '{self.stream_id}')")

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
                Statement(res, DCT + 'type', Literal(violation_type.name)),
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
        return hash(str(self.endpoint)
                    + str(self.node_id)
                    + str(self.stream_id)
                    + str(self.timestamp))

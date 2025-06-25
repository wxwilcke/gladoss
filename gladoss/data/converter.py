#!/usr/bin/env python

from datetime import datetime
import logging
from typing import Callable

from gladoss.core.multimodal.datatypes import XSD_NUMERIC, cast_literal_rev
from gladoss.core.stats import Distribution
from rdf.graph import Statement
from rdf.namespaces import OWL, RDF, RDFS, XSD
from rdf.terms import BNode, IRIRef, Literal, Resource

from gladoss.core.pattern import GraphPattern
from gladoss.core.validator import ValidationReport


logger = logging.getLogger(__name__)

DCT = IRIRef("http://purl.org/dc/terms/")
SH = IRIRef("https://www.w3.org/ns/shacl#")  # SHACL namespace


def report_to_graph(report: ValidationReport, mkid: Callable)\
        -> list[Statement]:
    """ Convert a validation report object to RDF graph in N-Triples format
        that conforms to the SHACL specification. Each detected anomaly
        (or error) is converted to a SHACL validation result with information
        about the causing assertion and with a detailed explanation. Some
        metadata is added to the head of the graph.

    :param report: [TODO:description]
    :param mkid: [TODO:description]
    :return: [TODO:description]
    """
    logger.info("Generating SHACL validation report for graph pattern "
                f"{report.pattern._id}")

    # default value
    conforms = True

    # define graph and metadata
    root = BNode('B' + mkid())
    graph = [
        Statement(root, RDF + 'type', SH + 'ValidationReport'),
        Statement(root, DCT + 'date', Literal(report.timestamp.isoformat(),
                                              datatype=XSD + 'dateTime')),
        Statement(root, DCT + 'identifier', Literal(report.pattern._id,
                                                    datatype=XSD + 'string')),
        Statement(root, DCT + 'conformsTo', Literal(
            "https://www.w3.org/TR/shacl/", datatype=XSD + 'anyURI'))
        ]

    # process violations and errors without associated shape
    for status_msg_lst in report.status_msg_lst:
        status_msg, status_msg_long, status_code = status_msg_lst

        res = BNode('B' + mkid())
        graph.extend([
            Statement(root, DCT + 'hasPart', res),
            Statement(res, RDFS + 'label',
                      Literal(status_msg, language="en")),
            Statement(res, SH + 'resultMessage',
                      Literal(status_msg_long, language="en"))
            ])

        sev = BNode('B' + mkid())
        graph.extend([
            Statement(res, SH + 'resultSeverity', sev),
            Statement(sev, RDF + 'type', SH + 'Severity'),
            Statement(sev, RDFS + 'label',
                      Literal(status_code.name, datatype=XSD + 'string')),
            Statement(sev, RDFS + 'comment',
                      Literal(status_code.description, language="en"))
            ])

        conforms = False

    # process violations and errors with associated shape
    # iterate over all validated shapes
    for ap_id, status_msg_lst in report.status_msg_lst_map.items():
        if ap_id not in report.apa_map.keys():
            continue

        # one result per anomaly
        assertion = report.apa_map[ap_id]  # type: Statement
        for status_msg, status_msg_long, status_code in status_msg_lst:
            res = BNode('B' + mkid())
            graph.extend([
                Statement(root, DCT + 'hasPart', res),
                Statement(res, RDF + 'type', SH + 'ValidationResult'),
                Statement(res, SH + 'focusNode', assertion.subject),
                Statement(res, SH + 'resultPath', assertion.predicate),
                Statement(res, SH + 'value', assertion.object),
                Statement(res, SH + 'sourceShape',
                          Literal(ap_id, datatype=XSD + 'string')),
                Statement(res, RDFS + 'label',
                          Literal(status_msg, language="en")),
                Statement(res, SH + 'resultMessage',
                          Literal(status_msg_long, language="en"))
                ])

            sev = BNode('B' + mkid())
            graph.extend([
                Statement(res, SH + 'resultSeverity', sev),
                Statement(sev, RDF + 'type', SH + 'Severity'),
                Statement(sev, RDFS + 'label',
                          Literal(status_code.name, datatype=XSD + 'string')),
                Statement(sev, RDFS + 'comment',
                          Literal(status_code.description, language="en"))
                ])

        conforms = False

    # summary of report - defaults to true if no anomalies have been found
    conforms = str(conforms).lower()
    graph.append(Statement(root, SH + 'conforms',
                           Literal(conforms, datatype=XSD + 'boolean')))

    return graph


def pattern_to_graph(mkid: Callable,
                     pattern: GraphPattern,
                     timestamp: datetime) -> list[Statement]:
    """ Convert graph pattern object to RDF graph in N-Triple format that
        conforms to the SHACL specification for shape graphs. Each assertion
        pattern is converted into a shape structure, with full preservation
        of semantics for fixed value constraints. Since SHACL does not support
        distributions, these are appropriated via min/max values for numerical
        data and an enumeration for all other forms of data. Metadata is added
        to the head of the graph.

    :param mkid: [TODO:description]
    :param pattern: [TODO:description]
    :param timestamp: [TODO:description]
    :return: [TODO:description]
    """
    logger.info("Generating SHACL shape graph for graph pattern "
                f"{pattern._id}")

    # define graph and metadata
    root = BNode('B' + mkid())
    graph = [
        Statement(root, RDF + 'type', OWL + 'Ontology'),
        Statement(root, DCT + 'date', Literal(timestamp.isoformat(),
                                              datatype=XSD + 'dateTime')),
        Statement(root, DCT + 'identifier', Literal(pattern._id,
                                                    datatype=XSD + 'string')),
        Statement(root, DCT + 'conformsTo', Literal(
            "https://www.w3.org/TR/shacl/", datatype=XSD + 'anyURI'))
            ]

    for ap_id in sorted(pattern.structure.keys()):
        ap = pattern.structure[ap_id]

        # shape for this assertion pattern
        shape = BNode('B' + ap_id)

        graph.extend([
            Statement(root, DCT + 'hasPart', shape),
            Statement(shape, RDF + 'type', SH + 'NodeShape'),
            Statement(shape, DCT + 'identifier',
                      Literal(ap_id, datatype=XSD + 'string')),
            Statement(shape, SH + 'targetClass', ap.anchor),
            Statement(shape, SH + 'targetSubjectsOf', ap.relation)
            ])

        pshape = BNode('B' + mkid())
        graph.extend([
            Statement(shape, SH + 'property', pshape),
            Statement(pshape, SH + 'path', ap.relation)
            ])

        # create constraints for RDF resources
        if isinstance(ap.value, Resource):
            graph.append(Statement(pshape, SH + 'hasValue', ap.value))
            if isinstance(ap.value, Literal):
                graph.append(Statement(pshape, SH + 'nodeKind', SH + 'Literal'))
                if ap.value.language is not None:
                    lst = BNode(mkid())
                    graph.extend([
                        Statement(pshape, SH + 'languageIn', lst),
                        Statement(lst, RDF + 'type', RDF + 'List'),
                        Statement(lst, RDF + 'first', ap.value.language),
                        Statement(lst, RDF + 'rest', RDF + 'nil')
                        ])
                elif ap.value.datatype is not None:
                    graph.append(
                        Statement(pshape, SH + 'datatype', ap.value.datatype))
            elif isinstance(ap.value, IRIRef):
                graph.append(Statement(pshape, SH + 'nodeKind', SH + 'IRI'))
            else:
                graph.append(Statement(
                    pshape, SH + 'nodeKind', SH + 'BlankNode'))

        # create constraints for distributions
        # These are approximations of the actual distribution due to
        # limitations of SHACL
        elif isinstance(ap.value, Distribution):
            if ap.value.dtype in XSD_NUMERIC:
                # approximate the distribution via min and max values
                v_min, v_max = min(ap.value.data), max(ap.value.data)

                # cast back to literal with appropriate format
                v_min = cast_literal_rev(v_min, ap.value.dtype, ap.value.lang)
                v_max = cast_literal_rev(v_max, ap.value.dtype, ap.value.lang)

                graph.extend([
                    Statement(pshape, SH + 'minInclusive', v_min),
                    Statement(pshape, SH + 'maxInclusive', v_max)
                    ])
            else:
                # create an RDF list with all unique values in the distribution
                lst = BNode(mkid())
                graph.extend([
                    Statement(pshape, SH + 'in', lst),
                    Statement(lst, RDF + 'type', RDF + 'List')
                    ])

                data_uniq = sorted(set(ap.value.data))
                for i, v in enumerate(data_uniq, 1):  # unique values
                    # cast back to literal with appropriate format
                    v = cast_literal_rev(v, ap.value.dtype, ap.value.lang)

                    lst_rest = RDF + 'nil'
                    if i < len(data_uniq):
                        lst_rest = BNode(mkid())
                    graph.extend(
                            [Statement(lst, RDF + 'first', v),
                             Statement(lst, RDF + 'rest', lst_rest)
                             ])

                    lst = lst_rest
        else:
            NotImplementedError()

    return graph

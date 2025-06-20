#!/usr/bin/env python

import logging
from typing import Callable

from gladoss.core.multimodal.datatypes import XSD_NUMERIC, cast_literal_rev
from gladoss.core.stats import Distribution
from rdf.graph import Statement
from rdf.namespaces import RDF, RDFS, XSD
from rdf.terms import BNode, IRIRef, Literal, Resource

from gladoss.core.pattern import GraphPattern
from gladoss.core.validator import ValidationReport


logger = logging.getLogger(__name__)

DCT = IRIRef("http://purl.org/dc/terms/")
SH = IRIRef("https://www.w3.org/ns/shacl#")  # SHACL namespace


def report_to_graph(report: ValidationReport, mkid: Callable)\
        -> set[Statement]:
    # shacl validation report
    pass

def pattern_to_graph(mkid: Callable,
                     pattern: GraphPattern) -> list[Statement]:
    root = BNode('B' + mkid())
    graph = [Statement(root, RDF + 'type', )
            ]
    # TODO: add graph ID, timestamp

    bag = BNode('B' + mkid())
    graph.extend([
        Statement(bag, RDF + 'type', RDF + 'Bag')
        ])
    for ap_id in sorted(pattern.structure.keys()):
        ap = pattern.structure[ap_id]

        shape = BNode('B' + ap_id)

        graph.extend([
            Statement(bag, RDFS + 'member', shape),
            Statement(shape, RDF + 'type', SH + 'NodeShape'),
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

        elif isinstance(ap.value, Distribution):
            if ap.value.dtype in XSD_NUMERIC:
                # approximate the distribution via min and max values
                v_min, v_max = min(ap.value.data), max(ap.value.data)

                # cast back to literal with appropriate format
                v_min = cast_literal_rev(v_min, ap.value.dtype, ap.value.lang)
                v_max = cast_literal_rev(v_max, ap.value.dtype, ap.value.lang)

                graph.extend([
                    Statement(pshape, SH + 'minExclusive', v_min),
                    Statement(pshape, SH + 'maxExclusive', v_max)
                    ])
            else:
                # create an RDF list with all unique values in the distribution
                lst = BNode(mkid())
                graph.extend([
                    Statement(pshape, SH + 'in', lst),
                    Statement(lst, RDF + 'type', RDF + 'List')
                    ])
                for v in set(ap.value.data):  # unique values
                    # cast back to literal with appropriate format
                    v = cast_literal_rev(v, ap.value.dtype, ap.value.lang)

                    lst_rest = BNode(mkid())
                    graph.extend(
                            [Statement(lst, RDF + 'first', v),
                             Statement(lst, RDF + 'rest', lst_rest)
                             ])

                    lst = lst_rest

                graph.append(Statement(lst, RDF + 'rest', RDF + 'nil'))
        else:
            NotImplementedError()

        graph.append(shape)

    return graph

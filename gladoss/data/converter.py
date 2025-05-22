#!/usr/bin/env python

import logging
from typing import Callable

from rdf.graph import Statement
from rdf.namespaces import RDF, RDFS, XSD
from rdf.terms import IRIRef, Literal

from gladoss.core.pattern import GraphPattern
from gladoss.core.validator import ValidationReport


logger = logging.getLogger(__name__)

BASE = IRIRef("https://example.org/")  # TODO: change
SH = IRIRef("https://www.w3.org/ns/shacl#")  # SHACL namespace


def report_to_graph(report: ValidationReport, mkid: Callable)\
        -> set[Statement]:
    # shacl validation report
    pass

def pattern_to_graph(mkid: Callable,
                     gpattern: GraphPattern) -> list[Statement]:
    graph = list()
    for ap in gpattern.pattern:
        shape = BASE + mkid()

        graph.extend(
                [Statement(shape, RDF + 'type', SH + 'NodeShape'),
                 Statement(shape, SH + 'targetObjectsOf', ap.relation),
                 ])


        # use sh:SPARQLTarget instead?
        # use n-quads; label is shape id

        graph.append(shape)

    return graph

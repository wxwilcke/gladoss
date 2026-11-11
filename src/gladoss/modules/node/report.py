#!/usr/bin/env python

from __future__ import annotations

import logging
from datetime import datetime
from enum import IntEnum
from functools import total_ordering

from gladoss_pm.data.utils import mknode, string_to_node
from rdf.graph import Statement
from rdf.namespaces import RDF, RDFS, XSD
from rdf.terms import IRIRef, Literal

logger = logging.getLogger(__name__)

BFO = IRIRef("http://purl.obolibrary.org/obo/")
DCT = IRIRef("http://purl.org/dc/terms/")
IOF = IRIRef("https://spec.industrialontologies.org/ontology/construct/")


class MaintenanceReport:
    @total_ordering
    class StatusCode(IntEnum):
        NOMINAL = 0, "Nominal Health"
        ERROR = 1, "Generic Error"
        NODATA = 2, "Insufficient Data"
        NONCRITIAL = 3, "Non-Critical Health Deterioration"
        CRITICAL = 4, "Critical Health Deterioration"

        def __new__(cls, *args, **kwds):
            obj = int.__new__(cls)
            obj._value_ = args[0]
            return obj

        # ignore the first param since it's already set by __new__
        def __init__(self, _: int, description: str):
            self._description_ = description

        def __eq__(self, other):
            if isinstance(other, MaintenanceReport.StatusCode):
                return self.value == other.value
            elif isinstance(other, int):
                return self.value == other
            else:
                raise TypeError()

        def __lt__(self, other):
            if isinstance(other, MaintenanceReport.StatusCode):
                return self.value < other.value
            elif isinstance(other, int):
                return self.value < other
            else:
                raise TypeError()

        def __gt__(self, other):
            if isinstance(other, MaintenanceReport.StatusCode):
                return self.value > other.value
            elif isinstance(other, int):
                return self.value > other
            else:
                raise TypeError()

        def __le__(self, other):
            if isinstance(other, MaintenanceReport.StatusCode):
                return self.value <= other.value
            elif isinstance(other, int):
                return self.value <= other
            else:
                raise TypeError()

        def __ge__(self, other):
            if isinstance(other, MaintenanceReport.StatusCode):
                return self.value >= other.value
            elif isinstance(other, int):
                return self.value >= other
            else:
                raise TypeError()

        # this makes sure that the description is read-only
        @property
        def description(self):
            return self._description_

    def __init__(self, node_id: str, stream_id: str,
                 timestamp: datetime,
                 status_code: MaintenanceReport.StatusCode,
                 status_msg: str):
        """ Report about the maintenance status of a registered node, together
            with the status code and message, and the timestamp.

        :param timestamp: [TODO:description]
        :param status_code: [TODO:description]
        :param status_msg_lst: [TODO:description]
        :param status_msg_lst_map: [TODO:description]
        """
        self.node_id = node_id
        self.stream_id = stream_id
        self.timestamp = timestamp
        self.status_code = status_code
        self.status_msg = status_msg

    def to_graph(self, namespace: str | None) -> list[Statement]:
        """ Convert a maintenance report object to RDF graph in N-Triples
            format.

            The output is of the following form:

            > :i rdf:type iof-constr:MaintenanceState ,
            >             iof-constr:OperatingState ;
            >    iof-constr:maintenanceStateOf :j ;
            >    rdfs:label { status code } ;
            >    dct:issued { date } .

            > :i rdf:type iof-constr:MaintenanceState ,
            >             iof-constr:DegradedState ;
            >    iof-constr:maintenanceStateOf :j ;
            >    rdfs:label { status code } ;
            >    dct:issued { date } ;
            >    bfo:BFO_0000054 [  # realized by
            >                     rdf:type iof-constr:UndesireableDisposition ;
            >                     iof-constr:describedBy [
            >                       rdf:type iof-constr:FailureModeCode ;
            >                       dct:description { ... } .
            >                      ]
            >                    ]

            > :i rdf:type iof-constr:MaintenanceState ,
            >             iof-constr:FailureState ;
            >    iof-constr:maintenanceStateOf :j ;
            >    rdfs:label { status code } ;
            >    dct:issued { date } ;
            >    bfo:BFO_0000054 [  # realized by
            >                     rdf:type iof-constr:UndesireableDisposition ;
            >                     iof-constr:describedBy [
            >                       rdf:type iof-constr:FailureModeCode ;
            >                       dct:description { ... } .
            >                      ]
            >                    ]

            > :j rdf:type iof-constr:MaintainableMaterialItem .  # device

        :param report: [TODO:description]
        :param mkid: [TODO:description]
        :return: [TODO:description]
        """
        logger.debug("Exporting maintenance report to RDF "
                     f"({self.node_id})")

        # use provided namespace for new nodes
        if isinstance(namespace, str) and len(namespace) > 0:
            if not namespace.endswith(('/', '#')):
                namespace += '#'

            namespace = IRIRef(namespace)

        # define graph and metadata
        root = mknode(namespace)
        graph = [
            Statement(root, RDF + 'type', IOF + 'MaintenanceState'),
            Statement(root, RDFS + 'label',
                      Literal(f"{self.status_code!s} Maintenance Report")),
            Statement(root, DCT + 'issued',
                      Literal(self.timestamp.isoformat(),
                              datatype=XSD + 'dateTime')),
            Statement(root, IOF + 'maintenanceStateOf',
                      string_to_node(self.node_id)),
            ]

        if self.status_code in [MaintenanceReport.StatusCode.NONCRITIAL,
                                MaintenanceReport.StatusCode.CRITICAL]:
            graph.extend([
                Statement(root, BFO + 'BFO_0000054', disp_node := mknode()),
                Statement(disp_node, RDF + 'type',
                          IOF + 'UndesireableDisposition'),
                Statement(disp_node, IOF + 'describedBy',
                          desc_node := mknode()),
                Statement(desc_node, RDF + 'type', IOF + 'FailureModeCode'),
                Statement(desc_node, DCT + 'description',
                          Literal(self.status_msg, language='en'))
                ])

            if self.status_code is MaintenanceReport.StatusCode.NONCRITIAL:
                graph.append(
                        Statement(root, RDF + 'type', IOF + 'DegradedState'))
            elif self.status_code is MaintenanceReport.StatusCode.CRITICAL:
                graph.append(
                        Statement(root, RDF + 'type', IOF + 'FailureState'))
        else:
            graph.append(Statement(root, RDF + 'type', IOF + 'OperatingState'))

        return graph

    def __hash__(self):
        return hash(str(self.node_id)
                    + str(self.timestamp))

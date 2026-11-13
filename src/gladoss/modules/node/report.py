#!/usr/bin/env python

from __future__ import annotations

import logging
from datetime import datetime

from rdf.graph import Statement
from rdf.namespaces import RDF, RDFS, XSD
from rdf.terms import IRIRef, Literal

from gladoss.core.report import ValidationReport
from gladoss.data.utils import mknode, string_to_node

logger = logging.getLogger(__name__)

BFO = IRIRef("http://purl.obolibrary.org/obo/")
DCT = IRIRef("http://purl.org/dc/terms/")
IOF = IRIRef("https://spec.industrialontologies.org/ontology/construct/")


class MaintenanceReport(ValidationReport):
    def __init__(self,
                 node_id: str,
                 stream_id: str,
                 timestamp: datetime,
                 status_code: ValidationReport.StatusCode,
                 status_msg_lst: list[tuple[str,
                                            str,
                                            ValidationReport.StatusCode,
                                            ValidationReport.ViolationType
                                            ]
                                      ]):
        """ Report about the maintenance status of a registered node, together
            with the status code and message, and the timestamp.

        :param timestamp: [TODO:description]
        :param status_code: [TODO:description]
        :param status_msg_lst: [TODO:description]
        :param status_msg_lst_map: [TODO:description]
        """
        super().__init__(
                type=ValidationReport.ReportType.NODE_VALIDATION_REPORT,
                node_id=node_id,
                stream_id=stream_id,
                timestamp=timestamp,
                status_code=status_code,
                status_msg_lst=status_msg_lst)

    def to_graph(self, namespace: str | None) -> list[Statement]:
        """ Convert a maintenance report object to RDF graph in N-Triples
            format.

            The output is of the following form:

            > :i rdf:type iof-constr:MaintenanceState ;
            >    iof-constr:maintenanceStateOf :j ;
            >    rdfs:label { status code } ;
            >    dct:issued { date } .

            > :i rdf:type iof-constr:MaintenanceState ;
            >    iof-constr:maintenanceStateOf :j ;
            >    rdfs:label { status code } ;
            >    dct:issued { date } ;
            >    bfo:BFO_0000054 [  # realized by
            >                 rdf:type iof-constr:UndesireableDisposition ;
            >                 iof-constr:describedBy [
            >                   rdf:type iof-constr:FailureModeCode ;
            >                   dct:type ?descriptionCode { violation type } ;
            >                   dct:description { ... } .
            >                  ]
            >                ]

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

        if self.status_code in [ValidationReport.StatusCode.NONCRITICAL,
                                ValidationReport.StatusCode.CRITICAL]:
            for status_msg_lst in self.status_msg_lst:
                _, status_msg_long, _, violation_type = status_msg_lst
                graph.extend([
                    Statement(root, BFO + 'BFO_0000054',
                              disp_node := mknode()),
                    Statement(disp_node, RDF + 'type',
                              IOF + 'UndesireableDisposition'),
                    Statement(disp_node, IOF + 'describedBy',
                              desc_node := mknode()),
                    Statement(desc_node, RDF + 'type',
                              IOF + 'FailureModeCode'),
                    Statement(desc_node, DCT + 'type',
                              Literal(violation_type.name)),
                    Statement(desc_node, DCT + 'description',
                              Literal(status_msg_long, language='en'))
                    ])

        #     if self.status_code is ValidationReport.StatusCode.NONCRITICAL:
        #         graph.append(
        #                 Statement(root, RDF + 'type', IOF + 'DegradedState'))
        #     elif self.status_code is ValidationReport.StatusCode.CRITICAL:
        #         graph.append(
        #                 Statement(root, RDF + 'type', IOF + 'FailureState'))
        # else:
        #     graph.append(Statement(root, RDF + 'type', IOF + 'OperatingState'))

        return graph

    def __hash__(self):
        return hash(str(self.node_id)
                    + str(self.timestamp))

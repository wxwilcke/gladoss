#!/usr/bin/env python

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Collection
from datetime import datetime
from enum import Enum, IntEnum, auto
from functools import total_ordering
from queue import Queue

from rdf.graph import Statement
from rdf.namespaces import RDF, RDFS, SHACL, XSD
from rdf.terms import IRIRef, Literal

from gladoss.data.utils import mknode, string_to_node
from gladoss.modules.graph.pattern import GraphPattern

logger = logging.getLogger(__name__)

DCT = IRIRef("http://purl.org/dc/terms/")

# symbols used in explanation
BECAUSE = '\N{BECAUSE}'
EMDASH = '\N{EM DASH}'
QED = '\N{END OF PROOF}'
PM = '\N{PLUS-MINUS SIGN}'
ELEMOF = '\N{ELEMENT OF}'


class ValidationReport:
    class ReportType(Enum):
        GRAPH_VALIDATION_REPORT = auto()
        STREAM_VALIDATION_REPORT = auto()

    class ViolationType(Enum):
        # Generic Violations
        ERR = auto()  # generic error
        DAT = auto()  # insufficient data

        # Graph Topology Violations
        GTV1 = auto()  # exact match violation (missing statements)
        GTV2 = auto()  # exact match violation (extra statements)
        GTV3 = auto()  # exact match violation (different statements)

        # Graph Semantics Violations
        GSV1 = auto()  # resource equality violation (IRI/BNode/Literal)
        GSV2 = auto()  # value equality violation (different IRI)
        GSV3 = auto()  # value equality violation (different literal value)
        GSV4 = auto()  # value equality violation (different data type)
        GSV5 = auto()  # value equality violation (different language tag)
        GSV6 = auto()  # value equality violation (discrete/continuous)

        # Graph Value Space Violations
        GVV1 = auto()  # critical value violation (unexpected value)
        GVV2 = auto()  # non-critical value violation (unexpected value)

        # Stream Health Violations
        SHV1 = auto()  # critical reception interval deviation
        SHV2 = auto()  # non-critical reception interval deviation

    @total_ordering
    class StatusCode(IntEnum):
        NOMINAL = 0, "Nominal Behaviour"
        ERROR = 1, "Generic Error"
        NODATA = 2, "Insufficient Data"
        INCONSISTENCY = 3, "Semantic Inconsistency"
        SUSPICIOUS = 4, "Non-Critical Anomaly"
        CRITICAL = 5, "Critical Anomaly"

        def __new__(cls, *args, **kwds):
            obj = int.__new__(cls)
            obj._value_ = args[0]
            return obj

        # ignore the first param since it's already set by __new__
        def __init__(self, _: int, description: str):
            self._description_ = description

        def __eq__(self, other):
            if isinstance(other, ValidationReport.StatusCode):
                return self.value == other.value
            elif isinstance(other, int):
                return self.value == other
            else:
                raise TypeError()

        def __lt__(self, other):
            if isinstance(other, ValidationReport.StatusCode):
                return self.value < other.value
            elif isinstance(other, int):
                return self.value < other
            else:
                raise TypeError()

        def __gt__(self, other):
            if isinstance(other, ValidationReport.StatusCode):
                return self.value > other.value
            elif isinstance(other, int):
                return self.value > other
            else:
                raise TypeError()

        def __le__(self, other):
            if isinstance(other, ValidationReport.StatusCode):
                return self.value <= other.value
            elif isinstance(other, int):
                return self.value <= other
            else:
                raise TypeError()

        def __ge__(self, other):
            if isinstance(other, ValidationReport.StatusCode):
                return self.value >= other.value
            elif isinstance(other, int):
                return self.value >= other
            else:
                raise TypeError()

        # this makes sure that the description is read-only
        @property
        def description(self):
            return self._description_

    def __init__(
            self,
            type: ValidationReport.ReportType,
            node_id: str,
            stream_id: str,
            timestamp: datetime,
            status_code: ValidationReport.StatusCode,
            graph_id: str | None = None,
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
        """ Report with results of an executed job with a (named) list
            of failed tests, the most pressing status code, and timestamp.

        :param timestamp: [TODO:description]
        :param status_code: [TODO:description]
        :param status_msg_lst: [TODO:description]
        :param status_msg_lst_map: [TODO:description]
        """
        self.type = type
        self.node_id = node_id
        self.stream_id = stream_id
        self.graph_id = graph_id
        self.timestamp = timestamp
        self.status_code = status_code
        self.status_msg_lst_map = status_msg_lst_map
        self.status_msg_lst = status_msg_lst


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

    def to_graph(self, namespace: str | None, mkid: Callable)\
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
        root = mknode(namespace, mkid)
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

            res = mknode(namespace, mkid)
            graph.extend([
                Statement(root, DCT + 'hasPart', res),
                Statement(res, RDF + 'type', SHACL + 'ValidationResult'),
                Statement(res, RDFS + 'label',
                          Literal(status_msg, language="en")),
                Statement(res, DCT + 'type', Literal(violation_type.name)),
                Statement(res, SHACL + 'resultMessage',
                          Literal(status_msg_long, language="en"))
                ])

            sev = mknode(namespace, mkid)
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
                res = mknode(namespace, mkid)
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

                sev = mknode(namespace, mkid)
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

    def to_graph(self, namespace: str | None, mkid: Callable)\
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
        root = mknode(namespace, mkid)
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

            res = mknode(namespace, mkid)
            graph.extend([
                Statement(root, DCT + 'hasPart', res),
                Statement(res, RDF + 'type', SHACL + 'ValidationResult'),
                Statement(res, RDFS + 'label',
                          Literal(status_msg, language="en")),
                Statement(res, DCT + 'type', Literal(violation_type.name)),
                Statement(res, SHACL + 'resultMessage',
                          Literal(status_msg_long, language="en"))
                ])

            sev = mknode(namespace, mkid)
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


class ReportScheduler:
    def __init__(self,
                 q_sheduler: Queue[tuple[str,
                                         ValidationReport,
                                         float
                                         ] | None
                                   ],
                 q_rapport: Queue[tuple[str,
                                        tuple[ValidationReport,
                                              int |
                                              list[int]
                                              ]
                                        ] | None
                                  ]):
        """ Thread safe class to manage scheduling of reports via timers.
            Adding and removing reports to the schedule is handled by a
            dedicated queue. Providing the same name to this queue will
            cancel previously scheduled reports known by that name; if
            a new report is provided as well then this report will take
            its place on the schedule.

        :param q_sheduler: [TODO:description]
        :param q_rapport: [TODO:description]
        """
        self.q_sheduler = q_sheduler
        self.q_rapport = q_rapport

        self.timers_active = {}  # type: dict[str, threading.Timer]
        self.enabled = False

    def _schedule(self):
        """ Process new scheduling jobs as they come in, by starting a
            timer that will push a report when the time triggers.
        """
        while True:
            job = self.q_sheduler.get()
            if job is None:
                break

            name, report, time = job

            # clean up completed timers
            self.timers_active = {name: timer
                                  for name, timer in self.timers_active.items()
                                  if timer.is_alive()}

            # stop and rmv active timer if registered
            if name in self.timers_active:
                self.timers_active[name].cancel()
                del self.timers_active[name]

            if time is None or report is None:
                # this was a cancel request
                logger.debug("Cancelled scheduled report "
                             f"(SID '{report.stream_id}')")

                continue

            assert isinstance(report, ValidationReport)
            assert type(time) is float

            # create and start new timer
            timer = threading.Timer(time,
                                    self.push_report,
                                    args=(report,))
            timer.start()

            # register timer
            self.timers_active[name] = timer

            logger.debug(f"Scheduled report for +{time:0.3g}s "
                         f"(SID '{report.stream_id}')")

    def enable(self):
        """ Start the report scheduler in a new thread.
        """
        if self.enabled:
            return

        logger.debug("Starting report scheduling daemon")
        self._thread = threading.Thread(target=self._schedule,
                                        name="scheduler")
        self._thread.start()

        self.enabled = True

    def disable(self):
        """ Terminate the report scheduler and cancel all scheduled reports.
        """
        if not self.enabled:
            return

        logger.debug("Stopping report scheduling daemon")
        self.q_sheduler.put(None)
        # stop timers and scheduler thread
        for timer in self.timers_active.values():
            timer.cancel()
            timer.join()

        self._thread.join()

        self.enabled = False

    def push_report(self, report: ValidationReport):
        """ Push a scheduled report to the report queue for publishing.

        :param report: [TODO:description]
        """
        thread_id = threading.current_thread().name

        logger.debug(f"Pushing scheduled report (SID '{report.stream_id}')")
        self.q_rapport.put((thread_id, (report, None)))

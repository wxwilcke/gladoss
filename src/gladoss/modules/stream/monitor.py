#! /usr/bin/env python

import logging
import threading
from collections.abc import Collection
from datetime import datetime, timedelta
from queue import Queue
from types import SimpleNamespace
from typing import Any

import numpy as np
from rdf.graph import Statement

from gladoss.core.report import ValidationReport
from gladoss.modules.processor import MessageProcessor
from gladoss.modules.stream.info_elem import StreamInfoElement
from gladoss.modules.stream.report import StreamValidationReport
from gladoss.modules.stream.store import MemoryStore
from gladoss.modules.stream.validator import validate_stream

logger = logging.getLogger(__name__)


class StreamProcessor(MessageProcessor):
    def __init__(self,
                 q_out: Queue,
                 q_shed: Queue,
                 rng: np.random.Generator,
                 cfg: SimpleNamespace,
                 **kwargs: dict | None):
        super().__init__(q_out=q_out, q_shed=q_shed, rng=rng, cfg=cfg)

        # initiate store which will manage and track stream characterictics
        self.store = MemoryStore(threading.RLock(), self.cfg.pattern_decay)

    def process(self,
                endpoint: str,
                node_id: str,
                stream_id: str,
                graph_id: str,
                graph_data: Collection[Statement],
                graph_label: int | list[int] | None,
                rtime: datetime)\
            -> None:
        """ Process various stream characteristics. This procedure will first
        try to validate the most recent characteristics using past data points,
        If no degradation is detected the characteristics are added to the
        record to be used as baseline for future validation requests. A
        validation report with is always returned.

        :param store: [TODO:description]
        :param stream_id: [TODO:description]
        :param endpoint: [TODO:description]
        :param rtime: [TODO:description]
        """
        if not self.cfg.evaluate_stream:
            return

        thread_id = threading.current_thread().name
        logger.info(f"Processing transmssion stream (SID '{stream_id}')")

        # register stream by identifier if needed
        with self.store._lock:
            if stream_id not in self.store.nodes:
                logger.debug("Associated transmssion stream not found "
                             f"(SID '{stream_id}')")
                logger.info("Registering new transmission stream "
                            f"(SID '{stream_id}')")
                if not self.store.register_node(stream_id):
                    logger.error("Unable to register or access transmission "
                                 f"stream (SID '{stream_id}')")

                return  # no need to evaluate the stream on first sight

        logger.debug("Associated transmission stream found "
                     f"(SID '{stream_id}')")

        # validate stream characteristics
        report, cache = self.create_validation_report(node_id, stream_id,
                                                      endpoint, rtime)
        if report.status_code in [ValidationReport.StatusCode.NOMINAL,
                                  ValidationReport.StatusCode.NODATA,
                                  ValidationReport.StatusCode.NONCRITICAL]:
            # update parameters if non-critical (to allow natural drift)
            if not self.update_stream_characteristics(
                    node_id, stream_id, endpoint, rtime, cache):
                logger.error("Encountered problems during stream "
                             f"parameters update (SID '{stream_id}')")
        else:
            logger.info(f"Transmission failed validation ('{stream_id}')")

        # always update reception time
        if not self.store.add(stream_id, StreamInfoElement.RTIME, rtime):
            logger.error("Unable to update transmission reception time "
                         f"(SID '{stream_id}')")

        self.q_out.put((thread_id, (report, None)))

    def create_validation_report(self, node_id: str, stream_id: str,
                                 endpoint: str, rtime: datetime)\
            -> tuple[StreamValidationReport, dict[StreamInfoElement, Any]]:
        """ Generate a validation report for the monitored stream from the
            provided node. This will start the validation procedure.

        :return: [TODO:description]
        """
        try:
            report, cache = validate_stream(self.store, node_id, stream_id,
                                            endpoint, rtime, self.cfg)
        except Exception as err:
            logger.error(f"Exception during stream validation: {err}")

            # create validation report without technical details
            status_msg = "Validation Malfunction"
            status_msg_long = ("An exception occurred during the evaluation "
                               f"of the stream from node '{node_id}' via "
                               f"endpoint '{endpoint}' ('{stream_id}.').")
            status_code = ValidationReport.StatusCode.ERROR,
            violation_type = ValidationReport.ViolationType.ERR
            report = StreamValidationReport(endpoint=endpoint,
                                            node_id=node_id,
                                            stream_id=stream_id,
                                            timestamp=rtime,
                                            status_code=status_code,
                                            status_msg_lst=[(status_msg,
                                                             status_msg_long,
                                                             status_code,
                                                             violation_type)])

        return report, cache

    def update_stream_characteristics(
            self,
            node_id: str,
            stream_id: str,
            endpoint: str,
            rtime: datetime,
            cache: dict[StreamInfoElement, Any])\
            -> bool:
        """ Derive and add new stream characteristics to the record.
            Return true if all characteristics have been added successfully.

            Schedule a delayed report if requested. This is handled during
            the update to avoid the schedule to be nonsensical due to an
            instable stream.

        :param store: [TODO:description]
        :param stream_id: [TODO:description]
        :param rtime: [TODO:description]
        :return: [TODO:description]
        """
        success = True
        logger.info(f"Updating transmission parameters (SID '{stream_id}')")

        # reception interval
        rtime_prev = self.store.most_recent(stream_id, StreamInfoElement.RTIME)
        if rtime_prev is not None:
            rinterval = (rtime - rtime_prev).total_seconds()  # type: float
            if not self.store.add(stream_id, StreamInfoElement.RINTERVAL,
                                  rinterval):
                success = False

            # schedule report for exceeding expected interval on next message
            if self.cfg.proactive_notification:
                schedule_rinterval_check(node_id, stream_id, endpoint,
                                         rtime, self.q_shed, cache)

        return success


def schedule_rinterval_check(node_id: str, stream_id: str, endpoint: str,
                             rtime: datetime, q_rpts: Queue,
                             cache: dict[StreamInfoElement, Any]) -> bool:
    """ Schedule a report publication at the moment of exceeding the
        expected reception interval of the next message from the
        specified node. This cancels a previously scheduled report
        about that node---calling this function confirms the reception
        of a previous message.

    :param stream_id: [TODO:description]
    :param endpoint: [TODO:description]
    :param rtime: [TODO:description]
    :param q_rpts: [TODO:description]
    :param cache: [TODO:description]
    :return: [TODO:description]
    """
    if (pi_rinterval := cache.get(StreamInfoElement.RINTERVAL)) is None:
        # within grace period or error during pi calculation
        logger.debug("Reception interval not found (within grace period?). "
                     "Cancelling scheduled check.")
        return False

    # upper critical limit in seconds
    try:
        _, rinterval_upper = pi_rinterval
    except ValueError as e:
        logger.error(f"Unable to extract pi_rinterval: {e}")

        return False

    # pretty formatted
    rinterval_td = timedelta(seconds=rinterval_upper)
    rinterval_str = ''.join([
        '' if rinterval_td.days <= 0
        else f"{rinterval_td.days} day(s) ",
        '' if rinterval_td.seconds < 60
        else f"{rinterval_td.seconds//60} minutes(s) ",
        '' if rinterval_td.seconds <= 0
        else f"{rinterval_td.seconds % 60} second(s) ",
        f"{rinterval_td.microseconds//1000} millisecond(s)"
        ])

    # create report
    status_msg = "Critical Reception Interval Deviation"
    status_msg_long = \
        "Message not received within expected interval "\
        f"of {rinterval_str} from node '{node_id}' via "\
        f"endpoint '{endpoint}' on stream '{stream_id}'."
    status_code = ValidationReport.StatusCode.CRITICAL
    violation_type = ValidationReport.ViolationType.SHV1

    report = StreamValidationReport(node_id=node_id,
                                    stream_id=stream_id,
                                    endpoint=endpoint,
                                    timestamp=rtime,
                                    status_code=status_code,
                                    status_msg_lst=[(status_msg,
                                                     status_msg_long,
                                                     status_code,
                                                     violation_type)])

    # schedule report for publication
    q_rpts.put((stream_id, report, rinterval_upper))

    logger.debug("Scheduled reception interval check.")

    return True

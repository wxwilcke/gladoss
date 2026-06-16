#! /usr/bin/env python

from datetime import datetime, timedelta
import logging
from queue import Queue
import threading
from types import SimpleNamespace
from typing import Any, Optional

from gladoss.core.report import StreamValidationReport, ValidationReport
from gladoss.core.stores import MemoryStore
from gladoss.modules.stream.info_elem import StreamInfoElement
from gladoss.modules.stream.validator import validate_stream


logger = logging.getLogger(__name__)


def schedule_rinterval_check(
        node_id: str, endpoint: str,
        rtime: datetime, q_rpts: Optional[Queue],
        cache: dict[ValidationReport.StreamInfoElement, Any]) -> bool:
    """
    [TODO:description]

    :param node_id: [TODO:description]
    :param endpoint: [TODO:description]
    :param rtime: [TODO:description]
    :param q_rpts: [TODO:description]
    :param cache: [TODO:description]
    :return: [TODO:description]
    """
    pi_rinterval = cache.get(ValidationReport.StreamInfoElement.RINTERVAL)
    if pi_rinterval is None:
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
        f"{rinterval_td.seconds % 60} second(s)",
        ])

    # create report
    status_msg = "Critical Reception Interval Deviation"
    status_msg_long = \
        "Message not received within expected interval "\
        f"of {rinterval_str} from '{endpoint}'"
    status_code = ValidationReport.StatusCode.CRITICAL

    report = StreamValidationReport(subject_id=node_id,
                                    endpoint=endpoint,
                                    timestamp=rtime,
                                    status_code=status_code,
                                    status_msg_lst=[(status_msg,
                                                     status_msg_long,
                                                     status_code)])

    # schedule report for publication
    q_rpts.put((node_id, report, rinterval_upper))

    return True


def update_stream_characteristics(
        store: MemoryStore, node_id: str, endpoint: str,
        rtime: datetime, q_rpts: Optional[Queue],
        cache: dict[ValidationReport.StreamInfoElement, Any])\
            -> bool:
    """ Derive and add new stream characteristics to the record.
        Return true if all characteristics have been added successfully.

        Schedule a delayed report if requested. This is handled during
        the update to avoid the schedule to be nonsensical due to an
        instable stream.

    :param store: [TODO:description]
    :param node_id: [TODO:description]
    :param rtime: [TODO:description]
    :return: [TODO:description]
    """
    success = True
    logger.info(f"Updating transmission parameters ({node_id})")

    # reception interval
    rtime_prev = store.most_recent(node_id, StreamInfoElement.RTIME)
    if rtime_prev is not None:
        rinterval = (rtime - rtime_prev).total_seconds()  # type: float
        if not store.add(node_id, StreamInfoElement.RINTERVAL, rinterval):
            success = False

        # schedule report for exceeding expected interval on next message
        if q_rpts is not None:  # requested scheduled report
            if not schedule_rinterval_check(node_id, endpoint, rtime,
                                            q_rpts, cache):
                logger.error("Unable to schedule reception interval report.")

    return success


def create_validation_report(store: MemoryStore, node_id: str, endpoint: str,
                             rtime: datetime, econf: SimpleNamespace)\
        -> tuple[StreamValidationReport,
                 tuple[ValidationReport.StreamInfoElement,
                       tuple[float, float]]]:
    """ Generate a validation report for the monitored stream from the
        provided node. This will start the validation procedure.

    :return: [TODO:description]
    """
    try:
        report, cache = validate_stream(store, node_id, endpoint, rtime, econf)
    except Exception as err:
        logger.error(f"Exception during transmission validation: {err}")

        # create validation report without technical detaiks (which are logged)
        status_msg = "Validation Malfunction"
        status_msg_long = "An exception occurred during the evaluation of "\
                          f"the monitored transmission from node '{node_id}.'"
        status_code = ValidationReport.StatusCode.ERROR
        report = StreamValidationReport(endpoint=endpoint,
                                        subject_id=node_id,
                                        timestamp=rtime,
                                        status_code=status_code,
                                        status_msg_lst=[(status_msg,
                                                         status_msg_long,
                                                         status_code)])

    return report, cache


def process_stream(store: MemoryStore, node_id: str, endpoint: str,
                   rtime: datetime,  econf: SimpleNamespace,
                   q_rpt: Queue, q_rpts: Optional[Queue])\
        -> None:
    """ Process various stream characteristics. This procedure will first
        try to validate the most recent characteristics using past data points,
        If no degradation is detected the characteristics are added to the
        record to be used as baseline for future validation requests. A
        validation report with is always returned.

    :param store: [TODO:description]
    :param node_id: [TODO:description]
    :param endpoint: [TODO:description]
    :param rtime: [TODO:description]
    """
    if not econf.evaluate_stream:
        return

    thread_id = threading.current_thread().name
    logger.info(f"Processing new transmission ({node_id})")

    # register graph by identifier if needed
    if node_id not in store.nodes:
        logger.debug("Associated stream not found "
                     f"({node_id})")
        logger.info(f"Registering new transmission stream ({node_id})")
        if not store.register_node(node_id):
            logger.error(f"Unable to register or access stream ({node_id})")

        return  # no need to evaluate the stream on first sight

    logger.debug(f"Associated stream found ({node_id})")

    # validate stream characteristics
    report, cache = create_validation_report(store, node_id, endpoint,
                                             rtime, econf)
    if report.status_code in [ValidationReport.StatusCode.NOMINAL,
                              ValidationReport.StatusCode.NODATA]:
        # stream healthy; update derived characteristics
        if not update_stream_characteristics(store, node_id, endpoint,
                                             rtime, q_rpts, cache):
            logger.error("Encountered problems during transmission "
                         f"parameters update ({node_id})")
    else:
        logger.info(f"Transmission failed validation ({node_id})")

    # always update reception time
    if not store.add(node_id, StreamInfoElement.RTIME, rtime):
        logger.error("Unable to update transmission reception time "
                     f"({node_id})")

    q_rpt.put((thread_id, (report, None)))

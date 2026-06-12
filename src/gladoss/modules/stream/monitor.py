#! /usr/bin/env python

from datetime import datetime
from enum import Enum, auto
import logging
from queue import Queue
import threading
from types import SimpleNamespace

from gladoss.core.report import StreamValidationReport, ValidationReport
from gladoss.core.stores import MemoryStore
from gladoss.modules.stream.validator import validate_stream


logger = logging.getLogger(__name__)


class StreamInfoElement(Enum):
    RINTERVAL = auto()  # reception interval
    RTIME = auto()  # reception time


def update_stream_characteristics(store: MemoryStore, node_id: str,
                                  rtime: datetime) -> bool:
    """ Add new stream characteristics to the record. Return true
        if all characteristics have been added successfully.

    :param store: [TODO:description]
    :param node_id: [TODO:description]
    :param rtime: [TODO:description]
    :return: [TODO:description]
    """
    success = True

    # reception time
    rtime_prev = store.most_recent(node_id, StreamInfoElement.RTIME)
    if not store.add(node_id, StreamInfoElement.RTIME, rtime):
        success = False

    # reception interval
    rinterval = (rtime - rtime_prev).total_seconds()  # type: float
    if not store.add(node_id, StreamInfoElement.RINTERVAL, rinterval):
        success = False

    return success


def create_validation_report(store: MemoryStore, node_id: str, endpoint: str,
                             rtime: datetime, econf: SimpleNamespace)\
        -> StreamValidationReport:
    """ Generate a validation report for the monitored stream from the
        provided node. This will start the validation procedure.

    :return: [TODO:description]
    """
    try:
        report = validate_stream(store, node_id, endpoint, rtime, econf)
    except Exception as err:
        logger.error(f"Exception during stream validation: {err}")

        # create validation report without technical detaiks (which are logged)
        status_msg = "Validation Malfunction"
        status_msg_long = "An exception occurred during the evaluation of "\
                          f"the monitored stream from node '{node_id}.'"
        status_code = ValidationReport.StatusCode.ERROR
        report = StreamValidationReport(endpoint=endpoint,
                                        subject_id=node_id,
                                        timestamp=rtime,
                                        status_code=status_code,
                                        status_msg_lst=[(status_msg,
                                                         status_msg_long,
                                                         status_code)])

    return report


def process_stream(store: MemoryStore, node_id: str, endpoint: str,
                   rtime: datetime,  econf: SimpleNamespace, q_rpt: Queue)\
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
    logger.debug(f"Processing stream characteristics ({node_id})")

    # register graph by identifier; continues if already registered
    if not store.register_node(node_id):
        return

    # validate stream characteristics
    report = create_validation_report(store, node_id, endpoint, rtime, econf)
    if report.status_code in [ValidationReport.StatusCode.NOMINAL,
                              ValidationReport.StatusCode.NODATA]:
        # stream healthy; update recorded characteristics
        if not update_stream_characteristics(store, node_id, rtime):
            logger.error("Encountered problems during stream characteristics "
                         f"update ({node_id})")
    else:
        logger.info(f"Stream health degradation detected ({node_id})")

    q_rpt.put((thread_id, (report, None)))

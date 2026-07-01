#! /usr/bin/env python

from datetime import datetime
import logging
from queue import Queue
import threading
from types import SimpleNamespace
from typing import Callable, Collection, Optional

import numpy as np
from rdf.graph import Statement

from gladoss.core.report import GraphValidationReport, ValidationReport
from gladoss.core.stores import PatternVault
from gladoss.core.utils import create_pattern_map
from gladoss.modules.graph.pattern import (AssertionPattern,
                                           GraphPattern,
                                           create_graph_pattern,
                                           update_graph_pattern)
from gladoss.modules.graph.validator import validate_state_graph


logger = logging.getLogger(__name__)


def create_validation_report(rng: np.random.Generator,
                             pattern: GraphPattern,
                             graph: Collection[Statement],
                             pattern_map: tuple[list[tuple[Statement,
                                                           AssertionPattern]],
                                                list[tuple[Statement,
                                                           AssertionPattern]],
                                                set[Statement]],
                             rtime: datetime,
                             econf: SimpleNamespace) -> GraphValidationReport:
    """ Generate a validation report for the observed state graph given
        the associated graph pattern. This will start the validation
        procedure.

    :param pattern: [TODO:description]
    :param graph: [TODO:description]
    :param econf: [TODO:description]
    :return: [TODO:description]
    """
    try:
        if pattern._t >= econf.grace_period:
            logger.info(f"Creating graph validation report ({pattern._id})")
        else:
            logger.info("Within grace period: skipping graph validation "
                        f"({pattern._id})")
        report = validate_state_graph(rng, pattern, graph, pattern_map,
                                      rtime, econf)
    except Exception as err:
        logger.error(f"Exception during graph validation: {err}")

        # convert to simpler form for validation report
        assertion_ap_pairs, _, _ = pattern_map
        apa_map = {ap._id: a for a, ap in assertion_ap_pairs}

        # create validation report without technical detaiks (which are logged)
        status_msg = "Validation Malfunction"
        status_msg_long = "An exception occurred during the evaluation of "\
                          f"the observed state graph with ID '{pattern._id}.'"
        status_code = ValidationReport.StatusCode.ERROR
        report = GraphValidationReport(subject_id=pattern._id,
                                       pattern=pattern, graph=graph,
                                       timestamp=rtime,
                                       apa_map=apa_map,
                                       status_code=status_code,
                                       status_msg_lst=[(status_msg,
                                                        status_msg_long,
                                                        status_code)])

    return report


def process_graph(rng: np.random.Generator, mkid: Callable,
                  pv: PatternVault, graph: Collection[Statement],
                  graph_id: str, graph_label: Optional[int | list[int]],
                  rtime: datetime,
                  pconf: SimpleNamespace, econf: SimpleNamespace,
                  q_rpt: Queue) -> None:
    """ Process an incoming message by finding the associated graph
        pattern, then evaluating the message with respect to this
        pattern, and, if OK, use the message to update the pattern.
        A new pattern is created if the message identifier is unknown,
        and a validation report is created and returned upon completion.

    :param pv: [TODO:description]
    :param graph: [TODO:description]
    :param graph_id: [TODO:description]
    :param config: [TODO:description]
    """
    thread_id = threading.current_thread().name
    logger.info(f"Processing new graph ({graph_id})")
    logger.debug(f" {{\n{'\n  '.join([str(s) for s in graph])}\n  }}")

    pattern = pv.find_associated_graph_pattern(graph_id)
    if pattern is None:
        logger.debug(f"Associated pattern not found ({graph_id})")
        pattern = create_graph_pattern(mkid=mkid, graph=graph,
                                       graph_id=graph_id,
                                       threshold=pconf.pattern_threshold,
                                       decay=pconf.pattern_decay)

        logger.debug("Registering new graph pattern at pattern vault "
                     f"({graph_id})")
        pv.add_graph_pattern(pattern)

        return  # no need to evaluate a graph on first sight

    logger.debug(f"Associated pattern found ({graph_id}) [t = {pattern._t}]")

    pattern_map = create_pattern_map(graph, pattern)
    report = create_validation_report(rng, pattern, graph, pattern_map,
                                      rtime, econf)
    if report.status_code in [ValidationReport.StatusCode.NOMINAL,
                              ValidationReport.StatusCode.NODATA,
                              ValidationReport.StatusCode.SUSPICIOUS]:
        # update parameters if non-critical (to allow natural drift)
        if pattern._t >= econf.grace_period:
            logger.info(f"Graph passed validation ({graph_id})")

        # either the state graph passed the validation check
        # or a non-critical deviation has been detected
        gpattern_upd = update_graph_pattern(mkid, pattern, graph,
                                            pattern_map, pconf)
        pv.update_graph_pattern(gpattern_upd, rtime)
    else:
        logger.info(f"Graph failed validation ({graph_id})")

    q_rpt.put((thread_id, (report, graph_label)))

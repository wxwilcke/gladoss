#! /usr/bin/env python

from datetime import datetime
import logging
from types import SimpleNamespace
from typing import Any

import numpy as np

from gladoss.core.stats import nonparametric_prediction_interval
from gladoss.core.stores import MemoryStore
from gladoss.core.report import (ValidationReport, StreamValidationReport,
                                 BECAUSE, EMDASH, QED, PM, ELEMOF)
from gladoss.modules.stream.info_elem import StreamInfoElement


logger = logging.getLogger(__name__)


def validate_stream(store: MemoryStore, node_id: str, endpoint: str,
                    rtime: datetime, econf: SimpleNamespace)\
        -> tuple[StreamValidationReport, dict[StreamInfoElement, Any]]:
    """ Validate the characteristics of a stream associated with a node
        and endpoint against historical data points. Returns a report
        with a list of failed validation tests (if applicable) and with
        the most pressing status code.

    :param store: [TODO:description]
    :param node_id: [TODO:description]
    :param endpoint: [TODO:description]
    :param rtime: [TODO:description]
    :return: [TODO:description]
    """
    cache_dct = dict()
    status_msg_lst = list()

    if node_id not in store.nodes:
        # historical data not available for this message stream
        logger.error("Aborted transmission validation: node unknown "
                     f"'{node_id}'")

        return (StreamValidationReport(
                 subject_id=node_id,
                 endpoint=endpoint,
                 timestamp=rtime,
                 status_code=ValidationReport.StatusCode.ERROR),
                cache_dct)

    # determine length of historical data points
    if len(store.get(node_id, StreamInfoElement.RTIME)) < econf.grace_period:
        logger.info("Within grace period: skipping transmission validation "
                    f"({node_id})")
    else:
        logger.info(f"Creating transmission validation report ({node_id})")

    # validate stream characteristics
    status_msg_upd, cache_upd\
        = validate_message_reception_interval(store, node_id, rtime,
                                              econf.alpha_critical,
                                              econf.alpha_suspicious,
                                              econf.pi_tolerance,
                                              econf.pi_right_sided,
                                              econf.grace_period)
    status_msg_lst.extend(status_msg_upd)
    cache_dct.update(cache_upd)

    # summarize validation results by highest code
    status_code_max = ValidationReport.StatusCode.NOMINAL
    for _, _, status_code in status_msg_lst:
        if status_code > status_code_max:
            status_code_max = status_code

            if status_code_max >= ValidationReport.StatusCode.CRITICAL:
                # no need to continue
                break

    logger.info(f"Transmission validation status {status_code_max.name} "
                f"({node_id})")

    return (StreamValidationReport(subject_id=node_id,
                                   endpoint=endpoint,
                                   timestamp=rtime,
                                   status_code=status_code_max,
                                   status_msg_lst=status_msg_lst),
            cache_dct)


def validate_message_reception_interval(store: MemoryStore,
                                        node_id: str,
                                        rtime: datetime,
                                        alpha_critical: float,
                                        alpha_suspicious: float,
                                        tolerance: float,
                                        right_sided: bool,
                                        grace_period: int)\
        -> tuple[tuple[list[tuple[str, str, ValidationReport.StatusCode]]],
                 dict[StreamInfoElement, tuple[float, float]]]:
    """ Test whether the reception interval of the latest message falls
        outside the computed symmetric non-parametric prediction interval
        (l, u] at the provided critical and suspicious levels, in which
        case the interval is flagged as a (non-) critical deviation.

        A tolerance value in [0, 1] can be provided which will expand the
        accepted range by a multiplication with the difference between the
        minimum and maximum values.

        Return the newly computed reception interval together with the status
        messages.

    :param store: [TODO:description]
    :param node_id: [TODO:description]
    :param rtime: [TODO:description]
    :param alpha_critical: [TODO:description]
    :param alpha_suspicious: [TODO:description]
    :param tolerance: [TODO:description]
    :return: [TODO:description]
    """
    cache_dct = dict()
    status_msg_lst = list()

    data_lst = store.get(node_id, StreamInfoElement.RINTERVAL)
    rtime_prev = store.most_recent(node_id, StreamInfoElement.RTIME)
    if rtime_prev is None or len(data_lst) < grace_period:
        logger.debug("Within grace period "
                     f"[t = {len(data_lst)} < {grace_period}]: "
                     "skipping transmission reception interval validation "
                     f"({node_id})")
        return status_msg_lst, cache_dct
    else:
        logger.debug(f"Validating stream reception interval ({node_id}; "
                     f"t = {len(data_lst)})")

    min_no_samples = 100  # hard lower limit for pi calculation
    if len(data_lst) < min_no_samples:
        status_msg = "Insufficient Data"
        status_msg_long = \
            "Insufficient messages have yet been received "\
            "to accurately establish nominal reception interval "\
            f"behaviour or deviations thereof {BECAUSE} "\
            f"OBSERVED: N = {len(data_lst)} {EMDASH} "\
            f"EXPECTED: N >= {min_no_samples} {QED}"
        status_code = ValidationReport.StatusCode.NODATA

        logger.info(status_msg_long)

        # skip further evaluation
        status_msg_lst.extend([(status_msg, status_msg_long, status_code)])

        return status_msg_lst, cache_dct

    # reception interval to validate
    rinterval_new = (rtime - rtime_prev).total_seconds()  # type: float

    # population to compute prediction interval from
    population = np.array(data_lst)

    prob_critical = 1. - alpha_critical
    prob_suspicious = 1. - alpha_suspicious

    pi_min, pi_max = 0., 0.  # pi + tol at critical level
    pi_lower, pi_upper = 0., 0.
    pi_violation = False
    for prob in [prob_critical, prob_suspicious]:
        # compute prediction interval (lower, upper]
        pi_lower, pi_upper = nonparametric_prediction_interval(population,
                                                               prob)

        # add tolerance
        pi_tol = abs(pi_upper - pi_lower) * tolerance
        pi_upper += pi_tol
        pi_lower -= pi_tol

        if prob == prob_critical:
            # save value for expected reception time of next message
            pi_min = pi_lower
            pi_max = pi_upper

        if rinterval_new > pi_upper\
                or (not right_sided and rinterval_new <= pi_lower):
            # outside of prediction interval
            pi_violation = True

            break  # forgo future tests if a violation is detected

    # infer validity from test results
    if pi_violation:
        if right_sided:
            pi_lower = -np.inf

        if prob == prob_critical:
            # observed value falls outside of prediction interval at the
            # critical level: this suggests the presence of a critical anomaly
            status_msg = "Critical Reception Interval Deviation"
            status_msg_long = \
                f"Reception interval not within {int((prob_critical) * 100)}"\
                f"% prediction interval {BECAUSE} "\
                f"EXPECTED: value {ELEMOF} ({pi_lower:0.3g}, {pi_upper:0.3g}]"\
                f" {PM} {pi_tol:0.3g} seconds "\
                f"{EMDASH} "\
                f"OBSERVED: {rinterval_new:0.3g} seconds {QED}"
            status_code = ValidationReport.StatusCode.CRITICAL

            status_msg_lst.extend([(status_msg, status_msg_long, status_code)])
            logger.info(status_msg_long)
        elif prob == 1 - alpha_suspicious:
            # observed value falls outside of prediction interval at
            # the suspicious level: this suggests the presence of a
            # non-critical anomaly
            status_msg = "Suspicious Reception Interval Deviation"
            status_msg_long = \
                "Reception interval not within "\
                f"{int((prob_suspicious) * 100)}% prediction interval "\
                f"{BECAUSE} "\
                f"EXPECTED: value {ELEMOF} ({pi_lower:0.3g}, {pi_upper:0.3g}]"\
                f" {PM} {pi_tol:0.3g} seconds "\
                f"{EMDASH} "\
                f"OBSERVED: {rinterval_new:0.3g} seconds {QED}"
            status_code = ValidationReport.StatusCode.SUSPICIOUS

            status_msg_lst.extend([(status_msg, status_msg_long, status_code)])
            logger.info(status_msg_long)

    cache_dct[StreamInfoElement.RINTERVAL] = (pi_min, pi_max)

    return status_msg_lst, cache_dct

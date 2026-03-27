#!/usr/bin/env python

from __future__ import annotations
from datetime import datetime
from enum import IntEnum
from functools import total_ordering
from itertools import chain
import logging
from types import SimpleNamespace
from typing import Callable, Collection, Optional

import numpy as np
from gladoss.data.converter import report_to_graph
from rdf.graph import Statement
from rdf.terms import IRIRef, Literal, Resource
from rdf.namespaces import XSD

from gladoss.core.multimodal.datatypes import (XSD_CONTINUOUS, XSD_DISCRETE,
                                               cast_literal, infer_datatype)
from gladoss.core.pattern import AssertionPattern, GraphPattern
from gladoss.core.stats import (ContinuousDistribution, DiscreteDistribution,
                                Distribution, HypothesisTest,
                                test_statistic_discrete,
                                test_statistic_continuous,
                                two_sample_hypothesis_test,
                                nonparametric_prediction_interval)


logger = logging.getLogger(__name__)

BECAUSE = '\N{BECAUSE}'
EMDASH = '\N{EM DASH}'
QED = '\N{END OF PROOF}'


def validate_state_graph(rng: np.random.Generator,
                         pattern: GraphPattern,
                         graph: Collection[Statement],
                         pattern_map: tuple[list[tuple[Statement,
                                                       AssertionPattern]],
                                            list[tuple[Statement,
                                                       AssertionPattern]],
                                            set[Statement]],
                         config: SimpleNamespace)\
        -> ValidationReport:
    """ Map all components of the observed state graph to the appropriate
        substructures in the associated pattern, and evaluate these components
        against the expected values or distributions.

    :param rng: [TODO:description]
    :param gpattern: [TODO:description]
    :param graph: [TODO:description]
    :param config: [TODO:description]
    :return: [TODO:description]
    """
    # validate structure, semantics, and data
    status_msg_lst_map, status_msg_lst\
        = validate_state_graph_components(rng, pattern, pattern_map, config)

    # summarize validation results by highest code
    status_code_max = ValidationReport.StatusCode.NOMINAL
    for _, _, status_code in chain.from_iterable([*status_msg_lst_map.values(),
                                                  status_msg_lst]):
        if status_code > status_code_max:
            status_code_max = status_code

            if status_code_max >= ValidationReport.StatusCode.CRITICAL:
                # no need to continue
                break

    logger.info(f"Validation status {status_code_max.name} "
                f"({pattern._id})")

    # convert to simpler form for validation report
    assertion_ap_pairs, _, _ = pattern_map
    apa_map = {ap._id: a for a, ap in assertion_ap_pairs}

    return ValidationReport(pattern=pattern,
                            graph=graph,
                            apa_map=apa_map,
                            timestamp=datetime.now(),
                            status_code=status_code_max,
                            status_msg_lst_map=status_msg_lst_map,
                            status_msg_lst=status_msg_lst)


def validate_state_graph_components(rng: np.random.Generator,
                                    pattern: GraphPattern,
                                    pattern_map: tuple[
                                        list[tuple[Statement,
                                                   AssertionPattern]],
                                        list[tuple[Statement,
                                                   AssertionPattern]],
                                        set[Statement]],
                                    config: SimpleNamespace)\
        -> tuple[dict[str, list[tuple[str, str, ValidationReport.StatusCode]]],
                 list[tuple[str, str, ValidationReport.StatusCode]]]:
    """ Validate all aspects of the observed state graph against the
        associated pattern.

    :param rng: [TODO:description]
    :param gpattern: [TODO:description]
    :param assertion_ap_pairs: [TODO:description]
    :param unmatched: [TODO:description]
    :param config: [TODO:description]
    :return: [TODO:description]
    """
    # find pairs of assertions and associated assertion patterns
    assertion_ap_pairs, _, unmatched = pattern_map

    status_msg_lst_data = dict()
    status_msg_lst_struc = list()
    if config.evaluate_structure:
        # validate the structure of the state graph
        if pattern._t < config.grace_period:
            logger.debug("In grace period: skipping graph structure "
                         f"validation ({pattern._id})")
        else:
            # no longer in learning phase
            logger.info(f"Validating graph structure ({pattern._id})")
            status_msg_lst_struc = validate_graph_structure(pattern,
                                                            assertion_ap_pairs,
                                                            unmatched,
                                                            config.match_cwa,
                                                            config.match_exact)

    if config.evaluate_data:
        # validate the data of the state graph, per component
        for i, (assertion, ap) in enumerate(assertion_ap_pairs, 1):
            if ap._t < config.grace_period:
                # still in learning phase
                logger.debug("In grace period: skipping graph data validation "
                             f"{i}/{len(assertion_ap_pairs)} ({pattern._id})")
                continue

            logger.info(f"Validating graph data {i}/{len(assertion_ap_pairs)} "
                        f"({pattern._id})")
            status_msg_lst = validate_graph_data(rng, assertion, ap,
                                                 config.alpha_critical,
                                                 config.alpha_suspicious,
                                                 config.evaluate_timestamps,
                                                 config.samplesize,
                                                 config.samplegap)

            status_msg_lst_data[ap._id] = status_msg_lst

    return status_msg_lst_data, status_msg_lst_struc


def validate_graph_data(rng: np.random.Generator,
                        assertion: Statement, ap: AssertionPattern,
                        alpha_critical: float, alpha_suspicious: float,
                        timestamp_eval: bool, samplesize: int,
                        interruption: int)\
        -> list[tuple[str, str, ValidationReport.StatusCode]]:
    """ Validate the data of a single assertion by checking resource and data
        types, comparing provided and expected values, and performing
        statistical tests. Mutiple validation checks are performed to provide
        a complete picture (even if one failed) expect for mutually exclusive
        ones.

    :param rng: [TODO:description]
    :param assertion: [TODO:description]
    :param ap: [TODO:description]
    :param alpha_critical: [TODO:description]
    :param alpha_suspicious: [TODO:description]
    :param samplesize: [TODO:description]
    :param interruption: [TODO:description]
    :return: [TODO:description]
    """
    status_msg_lst = list()
    if isinstance(ap.value, Resource):
        status_msg_lst = validate_graph_data_resource(assertion, ap)
    elif isinstance(ap.value, Distribution):
        if ap.value.num_samples < 100:
            status_msg = "Insufficient Data"
            status_msg_long = \
                "Insufficient samples have yet been observed "\
                "for this triple from the observed state graph "\
                "to establish nominal behaviour or deviations "\
                f"thereof. {BECAUSE} "\
                f"OBSERVED: N = {ap.value.num_samples} {EMDASH} "\
                f"EXPECTED: N >= 100 {QED}"
            status_code = ValidationReport.StatusCode.NODATA

            logger.info(status_msg_long)

            # skip further evaluation
            return [(status_msg, status_msg_long, status_code)]

        status_msg_lst = validate_graph_data_distribution(rng,
                                                          assertion, ap,
                                                          alpha_critical,
                                                          alpha_suspicious,
                                                          timestamp_eval,
                                                          samplesize,
                                                          interruption)

    return status_msg_lst


def validate_graph_data_discrete(assertion: Statement, ap: AssertionPattern,
                                 dtype_observed: Optional[IRIRef])\
        -> list[tuple[str, str, ValidationReport.StatusCode]]:
    """ Evaluate whether the provided resource fits to the associated discrete
        distribution, by checking resource type, data type, and the category of
        the data type against that of the expected distribution. This fuction
        does not test whether the observed value might have been drawn from the
        distribution that underlines the population.

    :param assertion: [TODO:description]
    :param ap: [TODO:description]
    :param dtype_observed: [TODO:description]
    :return: [TODO:description]
    """
    status_msg_lst = list()
    if isinstance(assertion.object, IRIRef)\
            and ap.value.dtype != XSD + 'anyURI':
        status_msg = "Resource Type Violation"
        status_msg_long = \
            "Observed resource type does not fit to expected distribution "\
            f"resource type. {BECAUSE} "\
            f"EXPECTED: '{'Literal' if ap.value.dtype is None
                          else ap.value.dtype}' {EMDASH} "\
            f"OBSERVED: '{type(assertion.object)}' {QED}"

        status_msg_lst.append((status_msg, status_msg_long,
                               ValidationReport.StatusCode.CRITICAL))
        logger.info(status_msg_long)
    elif isinstance(assertion.object, Literal):
        if dtype_observed not in XSD_DISCRETE:
            members_str = "{'" + "'; '".join(XSD_DISCRETE) + "'}"
            status_msg = "Value Type Violation"
            status_msg_long = \
                "Observed value type does not fit to expected distribution "\
                f"type. {BECAUSE} "\
                f"EXPECTED: one of {members_str} {EMDASH} "\
                f"OBSERVED: '{type(assertion.object)}' {QED}"

            status_msg_lst.append((status_msg, status_msg_long,
                                   ValidationReport.StatusCode.INCONSISTENCY))
            logger.info(status_msg_long)

        if ap.value.dtype is not None\
                and dtype_observed != ap.value.dtype:
            status_msg = "Data Type Violation"
            status_msg_long = \
                "Observed value data type does not fit to expected "\
                f"distribution data type. {BECAUSE} "\
                f"EXPECTED: '{'Unknown' if ap.value.dtype is None
                              else ap.value.dtype}' {EMDASH} "\
                f"OBSERVED: '{'Unknown' if dtype_observed is None
                              else dtype_observed}' {QED}"

            status_msg_lst.append((status_msg, status_msg_long,
                                   ValidationReport.StatusCode.INCONSISTENCY))
            logger.info(status_msg_long)

    return status_msg_lst


def validate_graph_data_continuous(assertion: Statement, ap: AssertionPattern,
                                   dtype_observed: Optional[IRIRef])\
        -> list[tuple[str, str, ValidationReport.StatusCode]]:
    """ Evaluate whether the provided resource fits to the associated
        continuous distribution, by checking resource type, data type, and the
        category of the data type against that of the expected distribution.
        This fuction does not test whether the observed value might have been
        drawn from the distribution that underlines the population.

    :param assertion: [TODO:description]
    :param ap: [TODO:description]
    :param dtype_observed: [TODO:description]
    :return: [TODO:description]
    """

    status_msg_lst = list()
    if not isinstance(assertion.object, Literal):
        status_msg = "Resource Type Violation"
        status_msg_long = \
            "Observed resource type does not fit to expected distribution "\
            f"resource type. {BECAUSE} "\
            f"EXPECTED: 'Literal' {EMDASH} "\
            f"OBSERVED: '{type(assertion.object)}' {QED}"

        status_msg_lst.append((status_msg, status_msg_long,
                               ValidationReport.StatusCode.CRITICAL))
        logger.info(status_msg_long)
    else:
        if dtype_observed not in XSD_CONTINUOUS:
            members_str = "{'" + "'; '".join(XSD_CONTINUOUS) + "'}"
            status_msg = "Value Type Violation"
            status_msg_long = \
                "Observed value type does not fit to expected distribution "\
                f"type. {BECAUSE} "\
                f"EXPECTED: one of {members_str} {EMDASH} "\
                f"OBSERVED: '{type(assertion.object)}' {QED}"

            status_msg_lst.append((status_msg, status_msg_long,
                                   ValidationReport.StatusCode.INCONSISTENCY))
            logger.info(status_msg_long)
        if ap.value.dtype is not None\
                and dtype_observed != ap.value.dtype:
            status_msg = "Data Type Violation"
            status_msg_long = \
                "Observed value data type does not fit to expected "\
                f"distribution data type. {BECAUSE} "\
                f"EXPECTED: '{'Unknown' if ap.value.dtype is None
                              else ap.value.dtype}' {EMDASH} "\
                f"OBSERVED: '{'Unknown' if dtype_observed is None
                              else dtype_observed}' {QED}"

            status_msg_lst.append((status_msg, status_msg_long,
                                   ValidationReport.StatusCode.INCONSISTENCY))
            logger.info(status_msg_long)

    return status_msg_lst


def validate_graph_data_distribution(rng: np.random.Generator,
                                     assertion: Statement,
                                     ap: AssertionPattern,
                                     alpha_critical: float,
                                     alpha_suspicious: float,
                                     timestamp_eval: bool,
                                     samplesize: int,
                                     interruption: int)\
        -> list[tuple[str, str, ValidationReport.StatusCode]]:
    """ Validate various aspects of a newly observed value against the
        distribution that belongs to the associated pattern.

    :param rng: [TODO:description]
    :param assertion: [TODO:description]
    :param ap: [TODO:description]
    :param alpha_critical: [TODO:description]
    :param alpha_suspicious: [TODO:description]
    :param samplesize: [TODO:description]
    :param interruption: [TODO:description]
    :raises NotImplementedError: [TODO:description]
    """
    # cast value to a suitable Python representation and infer data type
    value_new = None
    dtype_observed = None
    if isinstance(assertion.object, Literal):
        dtype_observed = infer_datatype(assertion.object)

        # cast value to appropriate format
        value_new = cast_literal(dtype_observed, assertion.object)
    elif isinstance(assertion.object, IRIRef):
        # use string value
        value_new = assertion.object.value
    else:
        raise NotImplementedError()

    # evaluate distribution types via meta data
    status_msg_lst = list()
    if isinstance(ap.value, DiscreteDistribution):
        test_statistic = test_statistic_discrete
        status_msg_lst.extend(
                validate_graph_data_discrete(assertion, ap,
                                             dtype_observed))
    elif isinstance(ap.value, ContinuousDistribution):
        test_statistic = test_statistic_continuous
        status_msg_lst.extend(
                validate_graph_data_continuous(assertion, ap,
                                               dtype_observed))
    else:
        raise NotImplementedError()

    if isinstance(ap.value, DiscreteDistribution)\
            and ap.value.fluidity() >= 1.0:
        # distribution only contains unique values which suggests a random
        # process. therefore skip statistical testing.
        logger.debug("Possible random process. Skipping statistical evaluation"
                     f" for '{assertion}'")
        return status_msg_lst

    # skip the statistical evaluation of timestamps
    if not timestamp_eval and ap.value.dtype in (XSD+'dateTime',
                                                 XSD+'datetimeStamp'):
        logger.debug("Encountered timestamp. Skipping statistical evaluation"
                     f" for '{assertion}'")
        return status_msg_lst

    # test whether a newly observed value falls outside the prediction interval
    if len(status_msg_lst) <= 0:  # if no previous violations have been found
        if isinstance(value_new, str):
            status_msg_lst.extend(
                    validate_graph_data_categorical(assertion, ap,
                                                    value_new))
        elif isinstance(value_new, int) or isinstance(value_new, float):
            status_msg_lst.extend(
                    validate_graph_data_numerical(assertion, ap,
                                                  value_new,
                                                  alpha_critical,
                                                  alpha_suspicious))
        else:
            raise NotImplementedError()

        # test whether the most recent n observed values are drawn from the
        # same distribution as those observed before.
        if ap._t % (samplesize//2) <= 0:
            # run this test infrequently as it can only detect deviations
            # between sets of values.
            status_msg_lst.extend(
                    validate_graph_data_distribution_fit(rng, assertion,
                                                         ap, value_new,
                                                         test_statistic,
                                                         alpha_critical,
                                                         alpha_suspicious,
                                                         samplesize,
                                                         interruption))

    return status_msg_lst


def validate_graph_data_categorical(assertion: Statement,
                                    ap: AssertionPattern,
                                    value_new: str)\
        -> list[tuple[str, str, ValidationReport.StatusCode]]:
    """ Test whether a newly observed value is a member of the set
        of nominal values, and, if this is not the case, flag the
        value as a critical anomaly.

        This test is designed for categorical data and can be used
        for point anomalies.

    :param assertion: [TODO:description]
    :param ap: [TODO:description]
    :param value_new: [TODO:description]
    :return: [TODO:description]
    """
    # TODO: this currently only checks on membership; a better
    #       approach would incorporate the likelyhood of observing
    #       the new value.

    population = np.array(ap.value.data)
    members = np.unique(population)

    status_msg_lst = list()
    if value_new not in members:
        members_str = "{'" + "'; '".join(members) + "'}"

        status_msg = "Critical Value Violation"
        status_msg_long = \
            "Observed value is not a member of the set of expected values "\
            f"{BECAUSE} "\
            f"EXPECTED: one of {members_str} "\
            f"OBSERVED: '{assertion.object}' {QED}"
        status_code = ValidationReport.StatusCode.CRITICAL

        status_msg_lst.append((status_msg, status_msg_long, status_code))
        logger.info(status_msg_long)

    return status_msg_lst


def validate_graph_data_numerical(assertion: Statement,
                                  ap: AssertionPattern,
                                  value_new: int | float,
                                  alpha_critical: float,
                                  alpha_suspicious: float)\
        -> list[tuple[str, str, ValidationReport.StatusCode]]:
    """ Test whether a newly observed value falls outside the computed
        symmetric non-parametric prediction interval (l, u] at the provided
        critical and suspicious levels, in which case the value is flagged
        as a (non-) critical anomaly.

        This test is designed for numerical data and can be used for point
        anomalies. Do not use this test when new observations are expected
        to fall outside of the data (eg instance IRIs and blank nodes).

    :param assertion: [TODO:description]
    :param ap: [TODO:description]
    :param value_new: [TODO:description]
    :param alpha_critical: [TODO:description]
    :param alpha_suspicious: [TODO:description]
    :return: [TODO:description]
    """
    population = np.array(ap.value.data)

    prob_critical = 1. - alpha_critical
    prob_suspicious = 1. - alpha_suspicious

    pi_lower, pi_upper = 0., 0.
    pi_violation = False
    for prob in [prob_critical, prob_suspicious]:
        # compute prediction interval (lower, upper]
        pi_lower, pi_upper = nonparametric_prediction_interval(population,
                                                               prob)

        if value_new <= pi_lower or value_new > pi_upper:
            # outside of prediction interval
            pi_violation = True

            break  # forgo future tests if a violation is detected

    # infer validity from test results
    status_msg_lst = list()
    if pi_violation:
        if prob == prob_critical:
            # observed value falls outside of prediction interval at the
            # critical level: this suggests the presence of a critical anomaly
            status_msg = "Critical Value Violation"
            status_msg_long = \
                f"Observed value not within {int((prob_critical) * 100)}"\
                f"% prediction interval. {BECAUSE} "\
                f"EXPECTED: value in ({pi_lower}, {pi_upper}] "\
                f"OBSERVED: '{assertion.object}' {QED}"
            status_code = ValidationReport.StatusCode.CRITICAL

            status_msg_lst.append((status_msg, status_msg_long, status_code))
            logger.info(status_msg_long)
        elif prob == 1 - alpha_suspicious:
            # observed value falls outside of prediction interval at
            # the suspicious level: this suggests the presence of a
            # non-critical anomaly
            status_msg = "Suspicious Value Violation"
            status_msg_long = \
                "Observed value not within "\
                f"{int((prob_suspicious) * 100)}% prediction interval. "\
                "{BECAUSE} "\
                f"EXPECTED: value in ({pi_lower}, {pi_upper}] "\
                f"OBSERVED: '{assertion.object}' {QED}"
            status_code = ValidationReport.StatusCode.SUSPICIOUS

            status_msg_lst.append((status_msg, status_msg_long, status_code))
            logger.info(status_msg_long)

    return status_msg_lst


def validate_graph_data_distribution_fit(rng: np.random.Generator,
                                         assertion: Statement,
                                         ap: AssertionPattern,
                                         value_new: str | int | float,
                                         test_statistic: Callable,
                                         alpha_critical: float,
                                         alpha_suspicious: float,
                                         samplesize: int, interruption: int)\
        -> list[tuple[str, str, ValidationReport.StatusCode]]:
    """ Test whether the n most recently observed values, including the newly
        observed value, follow the same underlying distribution as the oldest
        N - n observed values. The outcome of this test will be negative if the
        evidence suggests that the distributions differ at a critical
        significance level, and will be positive otherwise. Failure to pass at
        a suspicious significance level will trigger a warning.

        This test can be used for collective and contextual anomalies with the
        limitation that the anomaly occurs for a sufficiently long period (wrt
        sample size and interruption parameters) and that the anomaly is only
        detected with certainty at the end of that period.

    :param rng: [TODO:description]
    :param assertion: [TODO:description]
    :param ap: [TODO:description]
    :param value_new: [TODO:description]
    :param test_statistic: [TODO:description]
    :param alpha_critical: [TODO:description]
    :param alpha_suspicious: [TODO:description]
    :param samplesize: [TODO:description]
    :param interruption: [TODO:description]
    :return: [TODO:description]
    """
    # obtain most recent n samples and append the newly observed sample.
    # The result will be regarded as a sample of the population  that
    # will be evaluated against the true distribution.
    sample = np.array(ap.value.lastn(n=samplesize) + [value_new])

    # obtain all samples that are older than the most recent n samples,
    # with or without a brief interval in between to strengthen the
    # difference between these subsets. The result will be regarded as the
    # true distribution of the population.
    population = np.array(ap.value.data[:-samplesize - interruption])

    # test whether the sample might have been drawn from the same
    # distribution as the one underlying the population.
    num_samples = int(len(sample) * 0.67)  # arbitrary chosen amount
    (test_critical, test_suspicious), p_value\
        = two_sample_hypothesis_test(rng, sample_a=population,
                                     sample_b=sample,
                                     test_statistic_func=test_statistic,
                                     num_samples=num_samples,
                                     alpha_critical=alpha_critical,
                                     alpha_suspicious=alpha_suspicious)

    # infer validity from test results
    status_msg_lst = list()
    if test_critical == HypothesisTest.REJECT_H0:
        # enough evidence to reject the zero hypothesis that both samples were
        # drawn from the same underlying distribution at the critical level:
        # this suggests the presence of a critical anomaly
        status_msg = "Critical Pattern Violation"
        status_msg_long = \
            f"Last {samplesize} observed values differ significantly "\
            "from the expected pattern at the critical level. "\
            f"{BECAUSE} "\
            f"EXPECTED: Prob < {alpha_critical} "\
            f"{EMDASH} "\
            f"OBSERVED: Prob = {p_value:0.2f} {QED}"
        status_code = ValidationReport.StatusCode.CRITICAL

        status_msg_lst.append((status_msg, status_msg_long, status_code))
        logger.info(status_msg_long)
    elif test_suspicious == HypothesisTest.REJECT_H0:
        # enough evidence to reject the zero hypothesis that both samples were
        # drawn from the same underlying distribution at the suspicious level:
        # this might suggest the presence of an anomaly
        status_msg = "Suspicious Pattern Violation"
        status_msg_long = \
            f"Last {samplesize} observed values differ significantly "\
            "from the expected pattern at the suspicious level. "\
            f"{BECAUSE} "\
            f"EXPECTED: Prob < {alpha_suspicious} "\
            f"{EMDASH} "\
            f"OBSERVED: Prob = {p_value:0.2f} {QED}"

        status_code = ValidationReport.StatusCode.SUSPICIOUS

        status_msg_lst.append((status_msg, status_msg_long, status_code))
        logger.info(status_msg_long)

    return status_msg_lst


def validate_graph_data_resource(assertion: Statement, ap: AssertionPattern)\
        -> list[tuple[str, str, ValidationReport.StatusCode]]:
    """ Evaluate the resources of the observed state graph against the expected
        resources of the associated graph pattern, by checking the resource
        type, data type (in case of Literal), and exact value. This function is
        only called when the pattern has no distribution at this position.

    :param assertion: [TODO:description]
    :param ap: [TODO:description]
    :return: [TODO:description]
    """
    status_msg_lst = list()
    if type(ap.value) is not type(assertion.object):
        status_msg = "Resource Type Violation"
        status_msg_long = \
            "Observed resource type differs from expected resource type. "\
            f"{BECAUSE} "\
            f"EXPECTED: '{type(ap.value)}' {EMDASH} "\
            f"OBSERVED: '{type(assertion.object)}' {QED}"

        status_msg_lst.append((status_msg, status_msg_long,
                               ValidationReport.StatusCode.INCONSISTENCY))
        logger.info(status_msg_long)
    if isinstance(ap.value, IRIRef) and ap.value != assertion.object:
        status_msg = "Value Equality Violation"
        status_msg_long = \
            "Observed IRI value differs from expected IRI value. {BECAUSE} "\
            f"EXPECTED: '{ap.value}' {EMDASH} "\
            f"OBSERVED: '{assertion.object}' {QED}"

        status_msg_lst.append((status_msg, status_msg_long,
                               ValidationReport.StatusCode.CRITICAL))
        logger.info(status_msg_long)
    elif isinstance(ap.value, Literal):
        dtype_observed = infer_datatype(assertion.object)
        if dtype_observed != ap.value.datatype:
            status_msg = "Data Type Violation"
            status_msg_long = \
                "Observed Literal value data type differs from expected "\
                "literal value data type. {BECAUSE} "\
                f"EXPECTED: '{'Unknown' if ap.value.dtype is None
                              else ap.value.dtype}' {EMDASH} "\
                f"OBSERVED: '{'Unknown' if dtype_observed is None
                              else dtype_observed}' {QED}"

            status_msg_lst.append((status_msg, status_msg_long,
                                   ValidationReport.StatusCode.INCONSISTENCY))
            logger.info(status_msg_long)
        if assertion.object != ap.value:
            status_msg = "Value Equality Violation"
            status_msg_long = \
                "Observed Literal value differs from expected literal value. "\
                f"{BECAUSE} "\
                f"EXPECTED: '{ap.value}' {EMDASH} "\
                f"OBSERVED: '{assertion.object}' {QED}"

            status_msg_lst.append((status_msg, status_msg_long,
                                   ValidationReport.StatusCode.CRITICAL))
            logger.info(status_msg_long)
        if assertion.object.language is not None\
                and ap.value.language is not None\
                and assertion.object.language != ap.value.language:
            status_msg = "Value Language Violation"
            status_msg_long = \
                "Observed literal value language differs from expected "\
                f"literal value language. {BECAUSE} "\
                f"EXPECTED: '{ap.value.language}' {EMDASH} "\
                f"OBSERVED: '{assertion.object.language}' {QED}"

            status_msg_lst.append((status_msg, status_msg_long,
                                   ValidationReport.StatusCode.INCONSISTENCY))

    return status_msg_lst


def validate_graph_structure(pattern: GraphPattern,
                             assertion_ap_pairs: list[tuple[Statement,
                                                            AssertionPattern]],
                             unmatched: set[Statement],
                             match_cwa: bool, match_exact: bool)\
        -> list[tuple[str, str, ValidationReport.StatusCode]]:
    """ Validate the structure of the observed state graph, by checking
        possible mapped and unmapped sub-structures.

    :param pattern: [TODO:description]
    :param graph: [TODO:description]
    :param assertion_ap_pairs: [TODO:description]
    :param unmatched: [TODO:description]
    :param owa: [TODO:description]
    :param exact: [TODO:description]
    :return: [TODO:description]
    """
    status_msg_lst = list()
    if len(assertion_ap_pairs) < len(pattern):
        # there is an incomplete mapping between the graph and its pattern
        matched_str = "{'" + \
                      "'; '".join([a for a, _ in assertion_ap_pairs]) + \
                      "'}"
        if len(unmatched) > 0:
            unmatched_str = "{'" + "'; '".join(unmatched) + "'}"
            # not all assertions can be mapped to assertion patterns
            # this can occur if the structure or tail type is changed
            status_msg_long = \
                "Observed state graph contains triples that cannot be mapped "\
                "to any known subpattern of the associated graph pattern. "\
                f"{BECAUSE} "\
                f"OBSERVED: {matched_str} PLUS {unmatched_str} {EMDASH} "\
                f"EXPECTED: all of {pattern} {QED}"
            logger.info(status_msg_long)
        else:  # state graph is smaller than expected
            # this can occur when all assertions are mapped, but some assertion
            # patterns are still unpaired
            status_msg_long = \
                "Observed state graph contains less triples than required by "\
                f"the associated graph pattern. {BECAUSE} "\
                f"OBSERVED: {matched_str} {EMDASH} "\
                f"EXPECTED: all of {pattern} {QED}"
            logger.info(status_msg_long)

            if match_exact:
                status_msg = "Exact Match Requirement Violation."
                status_msg_lst.append(
                        (status_msg, status_msg_long,
                         ValidationReport.StatusCode.INCONSISTENCY))

        if match_cwa:
            # With CWA, we assume that missing pairs are false
            status_msg = "Closed-World Assumption Violation."
            status_msg_lst.append((status_msg, status_msg_long,
                                   ValidationReport.StatusCode.INCONSISTENCY))
    elif len(assertion_ap_pairs) == len(pattern):  # complete map
        if len(unmatched) > 0:
            unmatched_str = "{'" + "'; '".join(unmatched) + "'}"
            # all known assertions are matched, yet more are observed
            # these can be added as candidate assertion patterns if passed here
            status_msg_long = \
                "Observed state graph contains more triples than required by "\
                f"the associated graph pattern. {BECAUSE} "\
                f"OBSERVED: surplus {unmatched_str} {EMDASH} "\
                f"EXPECTED: all of {pattern} {QED}"
            logger.info(status_msg_long)

            if match_exact:
                # do not allow updates to structure
                status_msg = "Exact Match Requirement Violation."
                status_msg_lst.append(
                        (status_msg, status_msg_long,
                         ValidationReport.StatusCode.INCONSISTENCY))

    return status_msg_lst


class ValidationReport():
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
            self, pattern: GraphPattern,
            graph: Collection[Statement],
            apa_map: dict[str, Statement],
            timestamp: datetime,
            status_code: ValidationReport.StatusCode,
            status_msg_lst_map:
            dict[str, list[tuple[str, str, ValidationReport.StatusCode]]]
            = dict(),
            status_msg_lst:
            list[tuple[str, str, ValidationReport.StatusCode]]
            = list()):
        """ A validation report for an obversed state graph with its associated
            pattern, status code, and description of the evaluation results.

        :param pattern: [TODO:description]
        :param graph: [TODO:descriptions
        :param timestamp: [TODO:description]
        :param status_code: [TODO:description]
        :param status_msg: [TODO:description]
        :param status_msg_long: [TODO:description]
        """
        self.pattern = pattern
        self.graph = graph
        self.apa_map = apa_map
        self.timestamp = timestamp
        self.status_code = status_code
        self.status_msg_lst_map = status_msg_lst_map
        self.status_msg_lst = status_msg_lst

    def to_graph(self, mkid: Callable) -> list[Statement]:
        return report_to_graph(self, mkid)

    def __hash__(self):
        return hash(str(self.pattern)
                    + str(self.graph)
                    + str(self.timestamp))

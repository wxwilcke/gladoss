#!/usr/bin/env python

from __future__ import annotations
from datetime import datetime
from enum import Enum
import logging
from types import SimpleNamespace
from typing import Any, Callable, Collection, Optional

import numpy as np
from gladoss.data.converter import report_to_graph
from rdf.graph import Statement
from rdf.terms import IRIRef, Literal, Resource
from rdf.namespaces import RDFS

from gladoss.core.multimodal.datatypes import (XSD_CONTINUOUS, XSD_DISCRETE,
                                               cast_literal, infer_datatype)
from gladoss.core.pattern import AssertionPattern, GraphPattern
from gladoss.core.stats import (ContinuousDistribution, DiscreteDistribution,
                                Distribution, HypothesisTest,
                                test_statistic_continuous,
                                test_statistic_discrete,
                                two_sample_hypothesis_test)
from gladoss.core.utils import match_assertions_to_patterns


logger = logging.getLogger(__name__)


def validate_state_graph(rng: np.random.Generator,
                         gPattern: GraphPattern,
                         graph: Collection[Statement],
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
    # find pairs of assertions and associated assertion patterns
    # TODO: this is done later on as well; cache results?
    pattern_components = list(gPattern.structure.values())
    assertion_ap_pairs, unmatched\
        = match_assertions_to_patterns(graph, pattern_components)

    # default values
    status_msg_lst = list()
    status_msg_long_lst = list()
    status_code = ValidationReport.StatusCode.NOMINAL

    # check if the state graph can successfully be mapped to its pattern
    complete_map = True
    if len(assertion_ap_pairs) < len(gPattern) and len(unmatched) > 0:
        # missing matches in structure
        status_msg_lst = ["Mapping Error"]
        status_msg_long_lst = ["Unable to map all components of the "
                               "observed state graph to their association "
                               "substructure patterns. Skipping further "
                               "evaluation.\n Unmapped components: "
                               f"{', '.join(unmatched)}"]
        logger.warning(status_msg_long_lst[0])

        complete_map = False
        status_code = ValidationReport.StatusCode.ERROR

    # validate structure, semantics, and data
    if complete_map:
        valid_semantics, passed_critical, passed_suspicious, \
            status_msg_lst, status_msg_long_lst\
            = validate_state_graph_components(rng, gPattern,
                                              assertion_ap_pairs,
                                              unmatched, config)

        # order by most serious violation
        if not passed_critical:
            status_code = ValidationReport.StatusCode.CRITICAL
        elif not passed_suspicious:
            status_code = ValidationReport.StatusCode.SUSPICIOUS
        elif not valid_semantics:
            status_code = ValidationReport.StatusCode.INCONSISTENCY

    return ValidationReport(pattern=gPattern,
                            graph=graph,
                            timestamp=datetime.now(),
                            status_code=status_code,
                            status_msg=status_msg_lst,
                            status_msg_long=status_msg_long_lst)


def validate_state_graph_components(rng: np.random.Generator,
                                    gPattern: GraphPattern,
                                    assertion_ap_pairs: list[tuple],
                                    unmatched: set,
                                    config: SimpleNamespace)\
        -> tuple[bool, bool, bool, list[str], list[str]]:
    """ Validate all aspects of the observed state graph against the
        associated pattern.

    :param rng: [TODO:description]
    :param gpattern: [TODO:description]
    :param assertion_ap_pairs: [TODO:description]
    :param unmatched: [TODO:description]
    :param config: [TODO:description]
    :return: [TODO:description]
    """
    valid_lst = list()
    status_msg_lst = list()
    status_msg_long_lst = list()
    if config.evaluate_structure:
        # validate the structure of the state graph
        valid_semantics, status_msg_lst, status_msg_long_lst\
                = validate_graph_structure(gPattern,
                                           assertion_ap_pairs,
                                           unmatched,
                                           config.match_cwa,
                                           config.match_exact)

        valid_lst.append(valid_semantics)
        status_msg_lst.extend(status_msg_lst)
        status_msg_long_lst.extend(status_msg_long_lst)

    passed_critical, passed_suspicious = True, True
    if config.evaluate_data:
        # TODO: validate anchor
        # validate the data of the state graph, per component
        for assertion, ap in assertion_ap_pairs:
            valid_semantics, passed_critical, passed_suspicious, \
                    status_msg_lst, status_msg_long_lst\
                    = validate_graph_data(rng, assertion, ap,
                                          config.alpha_critical,
                                          config.alpha_suspicious,
                                          config.samplesize,
                                          config.samplegap)

            valid_lst.append(valid_semantics)
            status_msg_lst.extend(status_msg_lst)
            status_msg_long_lst.extend(status_msg_long_lst)

    # aggregate validation failures: fail if at least one test did not pass
    valid_semantics = False in valid_lst

    return valid_semantics, passed_critical, passed_suspicious, \
        status_msg_lst, status_msg_long_lst


def validate_graph_data(rng: np.random.Generator,
                        assertion: Statement, ap: AssertionPattern,
                        alpha_critical: float, alpha_suspicious: float,
                        samplesize: int, interruption: int)\
        -> tuple[bool, bool, bool, list[str], list[str]]:
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
    valid_semantics = True
    passed_critical = True
    passed_suspicious = True

    status_msg_lst = list()
    status_msg_long_lst = list()
    if isinstance(ap.value, Resource):
        valid_semantics, status_msg_lst, status_msg_long_lst\
                = validate_graph_data_resource(assertion, ap)
    elif isinstance(ap.value, Distribution):
        if ap.value.num_samples < samplesize * 2:
            msg = "Insufficient Data"
            msg_long = "Insufficient samples have yet been observed for this "\
                       "component of the observed state graph to establish "\
                       "nominal behaviour or deviations thereof."\
                       f"\n Observed: {str(assertion)}"
            logger.info(msg_long)

            return True, True, True, [msg], [msg_long]  # skip evaluation

        valid_semantics, passed_critical, passed_suspicious, \
            status_msg_lst, status_msg_long_lst\
            = validate_graph_data_distribution(rng, assertion, ap,
                                               alpha_critical,
                                               alpha_suspicious,
                                               samplesize, interruption)

    return valid_semantics, passed_critical, passed_suspicious, \
        status_msg_lst, status_msg_long_lst


def validate_graph_data_discrete(assertion: Statement, ap: AssertionPattern,
                                 dtype_observed: Optional[IRIRef])\
        -> tuple[bool, list[str], list[str]]:
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
    valid = True
    status_msg_lst = list()
    status_msg_long_lst = list()
    if isinstance(assertion.object, IRIRef)\
            and ap.value.dtype != RDFS + 'Resource':
        status_msg_long = "Observed resource type does not fit to "\
                          "expected distribution resource type."\
                          f"\n Expected: {'Unknown' if ap.value.dtype is
                                          None else ap.value.dtype}"\
                          f"\n Observed: {type(assertion.object)}"
        status_msg_long_lst.append(status_msg_long)
        valid = False

        status_msg_lst.append("Resource Type Violation")
        logger.info(status_msg_long)
    elif isinstance(assertion.object, Literal):
        if dtype_observed not in XSD_DISCRETE:
            status_msg_long = "Observed value type does not fit to "\
                              "expected distribution type."\
                              f"\n Expected: {type(ap.value)}"\
                              f"\n Observed: {type(assertion.object)}"

            status_msg_long_lst.append(status_msg_long)
            valid = False

            status_msg_lst.append("Value Type Violation")
            logger.info(status_msg_long)

        if ap.value.dtype is not None\
                and dtype_observed != ap.value.dtype:
            status_msg_long = "Observed value data type does not fit to "\
                              "expected distribution data type."\
                              f"\n Expected: {'Unknown' if ap.value.dtype is
                                              None else ap.value.dtype}"\
                              f"\n Observed: {'Unknown' if dtype_observed is
                                              None else dtype_observed}"

            status_msg_long_lst.append(status_msg_long)
            valid = False

            status_msg_lst.append("Data Type Violation")
            logger.info(status_msg_long)

    return valid, status_msg_lst, status_msg_long_lst


def validate_graph_data_continuous(assertion: Statement, ap: AssertionPattern,
                                   dtype_observed: Optional[IRIRef])\
        -> tuple[bool, list[str], list[str]]:
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

    valid = True
    status_msg_lst = list()
    status_msg_long_lst = list()
    if not isinstance(assertion.object, Literal):
        status_msg_long = "Observed resource type does not fit to "\
                          "expected distribution resource type."\
                          f"\n Expected: {type(ap.value)}"\
                          f"\n Observed: {type(assertion.object)}"

        status_msg_long_lst.append(status_msg_long)
        valid = False

        status_msg_lst.append("Resource Type Violation")
        logger.info(status_msg_long)
    else:
        if dtype_observed not in XSD_CONTINUOUS:
            status_msg_long = "Observed value type does not fit to "\
                              "expected distribution type."\
                              f"\n Expected: {type(ap.value)}"\
                              f"\n Observed: {type(assertion.object)}"

            status_msg_long_lst.append(status_msg_long)
            valid = False

            status_msg_lst.append("Value Type Violation")
            logger.info(status_msg_long)
        if ap.value.dtype is not None\
                and dtype_observed != ap.value.dtype:
            status_msg_long = "Observed value data type does not fit to "\
                              "expected distribution data type."\
                              f"\n Expected: {'Unknown' if ap.value.dtype is
                                              None else ap.value.dtype}"\
                              f"\n Observed: {'Unknown' if dtype_observed is
                                              None else dtype_observed}"

            status_msg_long_lst.append(status_msg_long)
            valid = False

            status_msg_lst.append("Data Type Violation")
            logger.info(status_msg_long)

    return valid, status_msg_lst,  status_msg_long_lst


def validate_graph_data_distribution(rng: np.random.Generator,
                                     assertion: Statement,
                                     ap: AssertionPattern,
                                     alpha_critical: float,
                                     alpha_suspicious: float,
                                     samplesize: int, interruption: int)\
        -> tuple[bool, bool, bool, list[str], list[str]]:
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
        value_new = cast_literal(dtype_observed, assertion.object.value)
    else:  # IRI
        value_new = assertion.object

    # evaluate distribution types via meta data
    valid_semantics = True
    test_statistic = None
    if isinstance(ap.value, DiscreteDistribution):
        test_statistic = test_statistic_discrete

        valid_semantics, status_msg_lst, status_msg_long_lst\
            = validate_graph_data_discrete(assertion, ap, dtype_observed)
    elif isinstance(ap.value, ContinuousDistribution):
        test_statistic = test_statistic_continuous

        valid_semantics, status_msg_lst, status_msg_long_lst\
            = validate_graph_data_continuous(assertion, ap, dtype_observed)
    else:
        raise NotImplementedError()

    # evaluate whether the new sample could've come from the same distribution
    # as the previously observed samples
    passed_critical, passed_suspicious = True, True
    if valid_semantics:  # if no previous violations have been detected
        passed_critical, passed_suspicious, \
                status_msg_lst, status_msg_long_lst\
                = validate_graph_data_distribution_fit(rng, assertion, ap,
                                                       value_new,
                                                       test_statistic,
                                                       alpha_critical,
                                                       alpha_suspicious,
                                                       samplesize,
                                                       interruption)

    return valid_semantics, passed_critical, passed_suspicious, \
        status_msg_lst, status_msg_long_lst


def validate_graph_data_distribution_fit(rng: np.random.Generator,
                                         assertion: Statement,
                                         ap: AssertionPattern,
                                         value_new: Any,
                                         test_statistic: Callable,
                                         alpha_critical: float,
                                         alpha_suspicious: float,
                                         samplesize: int, interruption: int)\
        -> tuple[bool, bool, list[str], list[str]]:
    """ Test whether the n most recently observed values, including the newly
        observed value, follow the same underlying distribution as the oldest
        N - n observed values. The outcome of this test will be negative if the
        evidence suggests that the distributions differ at a critical
        significance level, and will be positive otherwise. Failure to pass at
        a suspicious significance level will trigger a warning.

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
    test_critical, test_suspicious\
        = two_sample_hypothesis_test(rng, sample_a=population,
                                     sample_b=sample,
                                     test_statistic_func=test_statistic,
                                     num_samples=num_samples,
                                     alpha_critical=alpha_critical,
                                     alpha_suspicious=alpha_suspicious)

    # infer validity from test results
    passed_critical = True
    passed_suspicious = True
    status_msg = ""
    status_msg_long = ""
    if test_critical == HypothesisTest.REJECT_H0:
        # enough evidence to reject the zero hypothesis that both samples were
        # drawn from the same underlying distribution at the critical level:
        # this suggests the presence of a critical anomaly
        passed_critical = False

        status_msg = "Critical Value Violation"
        status_msg_long = "Evidence from the statistical evaluation suggests "\
                          "that this component of the observed state graph "\
                          "differs significantly from the associated graph "\
                          f"pattern at the critical level ({alpha_critical}):"\
                          f"\n Observed: {str(assertion)}"
    elif test_suspicious == HypothesisTest.REJECT_H0:
        # enough evidence to reject the zero hypothesis that both samples were
        # drawn from the same underlying distribution at the suspicious level:
        # this might suggest the presence of an anomaly
        passed_suspicious = False

        status_msg = "Suspicious Value Violation"
        status_msg_long = "Evidence from the statistical evaluation suggests "\
                          "that this component of the observed state graph "\
                          "differs significantly from the associated graph "\
                          "pattern at the suspicious level "\
                          f"({alpha_suspicious}):"\
                          f"\n Observed: {str(assertion)}"

    logger.info(status_msg_long)

    return passed_critical, passed_suspicious, [status_msg], [status_msg_long]


def validate_graph_data_resource(assertion: Statement, ap: AssertionPattern)\
        -> tuple[bool, list[str], list[str]]:
    """ Evaluate the resources of the observed state graph against the expected
        resources of the associated graph pattern, by checking the resource
        type, data type (in case of Literal), and exact value. This function is
        only called when the pattern has no distribution at this position.

    :param assertion: [TODO:description]
    :param ap: [TODO:description]
    :return: [TODO:description]
    """
    valid = True
    status_msg_lst = list()
    status_msg_long_lst = list()
    if type(ap.value) is not type(assertion.object):
        status_msg_long = "Observed resource type differs from expected "\
                          "resource type."\
                          f"\n Expected: {type(ap.value)}"\
                          f"\n Observed: {type(assertion.object)}"
        status_msg_long_lst.append(status_msg_long)
        valid = False

        status_msg_lst.append("Resource Type Violation")
        logger.info(status_msg_long)
    if isinstance(ap.value, IRIRef) and ap.value != assertion.object:
        status_msg_long = "Observed IRI value differs from expected IRI "\
                          "value."\
                          f"\n Expected: {ap.value}"\
                          f"\n Observed: {assertion}"
        status_msg_long_lst.append(status_msg_long)
        valid = False

        status_msg_lst.append("Value Equality Violation")
        logger.info(status_msg_long)
    elif isinstance(ap.value, Literal):
        dtype_observed = infer_datatype(assertion.object)
        if dtype_observed != ap.value.dtype:
            status_msg_long = "Observed Literal value data type differs from "\
                              "expected Literal value data type."\
                              f"\n Expected: {'Unknown' if ap.value.dtype is
                                              None else ap.value.dtype}"\
                              f"\n Observed: {'Unknown' if dtype_observed is
                                              None else dtype_observed}"
            status_msg_long_lst.append(status_msg_long)
            valid = False

            status_msg_lst.append("Data Type Violation")
            logger.info(status_msg_long)
        if assertion.object != ap.value:
            status_msg_long = "Observed Literal value differs from expected "\
                              "Literal value."\
                              f"\n Expected: {ap.value}"\
                              f"\n Observed: {assertion}"
            status_msg_long_lst.append(status_msg_long)
            valid = False

            status_msg_lst.append("Value Equality Violation")
            logger.info(status_msg_long)

    return valid, status_msg_lst, status_msg_long_lst


def validate_graph_structure(gPattern: GraphPattern,
                             assertion_ap_pairs: list[tuple], unmatched: set,
                             match_cwa: bool, match_exact: bool)\
        -> tuple[bool, list[str], list[str]]:
    """ Validate the structure of the observed state graph, by checking
        possible mapped and unmapped sub-structures.

    :param gPattern: [TODO:description]
    :param graph: [TODO:description]
    :param assertion_ap_pairs: [TODO:description]
    :param unmatched: [TODO:description]
    :param owa: [TODO:description]
    :param exact: [TODO:description]
    :return: [TODO:description]
    """
    valid = True
    status_msg_lst = list()
    status_msg_long_lst = list()
    if len(assertion_ap_pairs) < len(gPattern) and len(unmatched) <= 0:
        # incomplete mapping, yet no unmatched assertions remain
        status_msg_long = "Observed state graph contains less components "\
                          "than required by the associated graph "\
                          f"pattern: {len(assertion_ap_pairs)} < "\
                          f"{len(gPattern)}"
        logger.info(status_msg_long)

        if match_exact:
            status_msg_lst.append("Exact Match Requirement Violation.")
            status_msg_long_lst.append(status_msg_long)
            valid = False

        if match_cwa:
            status_msg_lst.append("Closed-World Assumption Violation.")
            status_msg_long_lst.append(status_msg_long)
            valid = False

    elif len(assertion_ap_pairs) == len(gPattern) and len(unmatched) > 0:
        # all known assertions are matched, yet more are observed
        status_msg_long = "Observed state graph contains more components "\
                          "than required by the associated graph "\
                          f"pattern: {len(assertion_ap_pairs)} > "\
                          f"{len(gPattern)}"
        logger.info(status_msg_long)

        if match_exact:
            # do not allow updates to structure
            status_msg_lst.append("Exact Match Requirement Violation.")
            status_msg_long_lst.append(status_msg_long)
            valid = False

    return valid, status_msg_lst, status_msg_long_lst


class ValidationReport():
    class StatusCode(Enum):
        NOMINAL = 0  # p-value above non-critical significance level
        ERROR = 1  # error during evaluation
        INCONSISTENCY = 2  # structural or semantic inconsistency
        SUSPICIOUS = 3  # p-value under non-critical significance level
        CRITICAL = 4  # p-value under critical significance level

    def __init__(self, pattern: GraphPattern, graph: Collection[Statement],
                 timestamp: datetime, status_code: StatusCode,
                 status_msg: str | list, status_msg_long: str | list):
        """ A validation report for an obversed state graph with its associated
            pattern, status code, and description of the evaluation results.

        :param pattern: [TODO:description]
        :param graph: [TODO:description]
        :param timestamp: [TODO:description]
        :param status_code: [TODO:description]
        :param status_msg: [TODO:description]
        :param status_msg_long: [TODO:description]
        """
        self.pattern = pattern
        self.graph = graph
        self.timestamp = timestamp
        self.status_code = status_code

        self.status_msg = [status_msg]\
            if isinstance(status_msg, str) else status_msg
        self.status_msg_long = [status_msg_long]\
            if isinstance(status_msg_long, str) else status_msg_long

    def to_graph(self, mkid: Callable) -> set[Statement]:
        return report_to_graph(self, mkid)

    def __hash__(self):
        return hash(str(self.pattern)
                    + str(self.graph)
                    + ', '.join(self.status_msg_long)
                    + str(self.timestamp))

    def __str__(self):
        return '\n'.join(self.status_msg_long)

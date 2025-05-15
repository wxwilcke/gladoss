#!/usr/bin/env python

from __future__ import annotations
from datetime import datetime
from enum import Enum
import logging
from types import SimpleNamespace
from typing import Collection, Optional

import numpy as np
from rdf.graph import Statement
from rdf.terms import IRIRef, Literal, Resource
from rdf.namespaces import RDFS

from gladoss.core.multimodal.datatypes import (XSD_CONTINUOUS, XSD_DISCRETE,
                                               cast_literal, infer_datatype)
from gladoss.core.pattern import AssertionPattern, GraphPattern
from gladoss.core.stats import (ContinuousDistribution, DiscreteDistribution,
                                Distribution, test_statistic_continuous,
                                test_statistic_discrete,
                                two_sample_hypothesis_test)
from gladoss.core.utils import match_facts_to_patterns


logger = logging.getLogger(__name__)


def validate_state_graph(rng: np.random.Generator,
                         gpattern: GraphPattern,
                         graph: Collection[Statement],
                         config: SimpleNamespace)\
        -> Optional[ValidationReport]:
    # find pairs of facts and associated assertion patterns
    fact_ap_pairs, unmatched = match_facts_to_patterns(
            graph, gpattern.pattern)

    if len(fact_ap_pairs) < len(gpattern) and len(unmatched) > 0:
        status_msg_long = "Unable to map all components of the observed "\
                          "state graph to their association substructure "\
                          "patterns. Skipping further evaluation. "\
                          f"Unmapped components: {', '.join(unmatched)}."
        logger.warning(status_msg_long)

        # skip further evaluation
        return ValidationReport(pattern=gpattern,
                                graph=graph,
                                timestamp=datetime.now(),
                                status_code=ValidationReport.StatusCode.ERROR,
                                status_msg="Mapping Error",
                                status_msg_long=status_msg_long)

    status_msg_lst = list()
    status_msg_long_lst = list()
    if config.evaluate_structure:
        # validate the structure of the state graph
        msg_lst, msg_long_lst = validate_graph_structure(gpattern,
                                                         fact_ap_pairs,
                                                         unmatched,
                                                         config.match_cwa,
                                                         config.match_exact)

        status_msg_lst.extend(msg_lst)
        status_msg_long_lst.extend(msg_long_lst)

    # TODO: skip is dist size is too low
    if config.evaluate_data:
        for fact, ap in fact_ap_pairs:
            valid, status_msg_long = validate_graph_data(rng, fact, ap,
                                                         config.samplesize,
                                                         config.samplegap)

    return report


def validate_graph_data(rng: np.random.Generator,
                        fact: Statement, ap: AssertionPattern,
                        samplesize: int, interruption: int)\
        -> tuple[bool, str]:
    valid = True
    msg_long_lst = list()

    if isinstance(ap.tail, Resource):
        valid, msg_long_lst = validate_graph_data_resource(fact, ap)

    if isinstance(ap.tail, Distribution):
        x = validate_graph_data_distribution(rng, fact, ap,
                                             samplesize, interruption)


def validate_graph_data_discrete(fact: Statement, ap: AssertionPattern,
                                 dtype_observed: Optional[IRIRef])\
        -> tuple[bool, list[str]]:
    valid = True
    status_msg_long_lst = list()
    if isinstance(fact.object, IRIRef) and ap.tail.dtype != RDFS+'Resource':
        status_msg_long = "Observed value data type does not fit to "\
                          "expected distribution data type."\
                          f"\n Expected: {'Unknown' if ap.tail.dtype is
                                          None else ap.tail.dtype}"\
                          f"\n Observed: {type(fact.object)}"
        status_msg_long_lst.append(status_msg_long)
        valid = False

        logger.info(status_msg_long)
    elif isinstance(fact.object, Literal):
        if dtype_observed not in XSD_DISCRETE:
            status_msg_long = "Observed value type does not fit to "\
                              "expected distribution type."\
                              f"\n Expected: {type(ap.tail)}"\
                              f"\n Observed: {type(fact.object)}"

            status_msg_long_lst.append(status_msg_long)
            valid = False

            logger.info(status_msg_long)

        if ap.tail.dtype is not None\
                and dtype_observed != ap.tail.dtype:
            status_msg_long = "Observed value data type does not fit to "\
                              "expected distribution data type."\
                              f"\n Expected: {'Unknown' if ap.tail.dtype is
                                              None else ap.tail.dtype}"\
                              f"\n Observed: {'Unknown' if dtype_observed is
                                              None else dtype_observed}"

            status_msg_long_lst.append(status_msg_long)
            valid = False

            logger.info(status_msg_long)

    return valid, status_msg_long_lst


def validate_graph_data_continuous(fact: Statement, ap: AssertionPattern,
                                   dtype_observed: Optional[IRIRef])\
        -> tuple[bool, list[str]]:
    valid = True
    status_msg_long_lst = list()
    if not isinstance(fact.object, Literal):
        status_msg_long = "Observed value type does not fit to "\
                          "expected distribution type."\
                          f"\n Expected: {type(ap.tail)}"\
                          f"\n Observed: {type(fact.object)}"

        status_msg_long_lst.append(status_msg_long)
        valid = False

        logger.info(status_msg_long)
    else:
        if dtype_observed not in XSD_CONTINUOUS:
            status_msg_long = "Observed value type does not fit to "\
                              "expected distribution type."\
                              f"\n Expected: {type(ap.tail)}"\
                              f"\n Observed: {type(fact.object)}"

            status_msg_long_lst.append(status_msg_long)
            valid = False

            logger.info(status_msg_long)
        if ap.tail.dtype is not None\
                and dtype_observed != ap.tail.dtype:
            status_msg_long = "Observed value data type does not fit to "\
                              "expected distribution data type."\
                              f"\n Expected: {'Unknown' if ap.tail.dtype is
                                              None else ap.tail.dtype}"\
                              f"\n Observed: {'Unknown' if dtype_observed is
                                              None else dtype_observed}"

            status_msg_long_lst.append(status_msg_long)
            valid = False

            logger.info(status_msg_long)

    return valid, status_msg_long_lst


def validate_graph_data_distribution(rng: np.random.Generator,
                                     fact: Statement, ap: AssertionPattern,
                                     samplesize: int, interruption: int):
    dtype_observed = None
    if isinstance(fact.object, Literal):
        dtype_observed = infer_datatype(fact.object)

    value_new = None
    test_statistic = None
    if isinstance(ap.tail, DiscreteDistribution):
        valid, msg_long_lst\
                = validate_graph_data_discrete(fact, ap, dtype_observed)

        test_statistic = test_statistic_discrete
        if isinstance(fact.object, Literal):
            # cast value to appropriate format
            value_new = cast_literal(dtype_observed, fact.object.value)
        else:  # IRI
            value_new = fact.object

    elif isinstance(ap.tail, ContinuousDistribution):
        valid, msg_long_lst\
                = validate_graph_data_continuous(fact, ap, dtype_observed)

        test_statistic = test_statistic_continuous

        # cast value to appropriate format
        value_new = cast_literal(dtype_observed, fact.object.value)
    else:
        raise NotImplementedError()

    # obtain most recent n samples and append the newly observed sample.
    # The result will be regarded as a sample of the population  that
    # will be evaluated against the true distribution.
    sample = ap.tail.lastn(n=samplesize) + [value_new]

    # obtain all samples that are older than the most recent n samples,
    # with or without a brief interval in between to strengthen the
    # difference between these subsets. The result will be regarded as the
    # true distribution of the population.
    population = ap.tail.data[:-samplesize - interruption]

    # test whether the sample might have been drawn from the same
    # distribution as the one underlying the population.
    num_samples = int(len(sample) * 0.67)
    test = two_sample_hypothesis_test(rng, population, sample,
                                      test_statistic, num_samples)


def validate_graph_data_resource(fact: Statement, ap: AssertionPattern)\
        -> tuple[bool, list[str]]:
    """ Evaluate the resources of the observed state graph against the expected
        resources of the associated graph pattern, by checking the resource type,
        data type (in case of Literal), and exact value. This function is only
        called when the pattern has no distribution at this position.

    :param fact: [TODO:description]
    :param ap: [TODO:description]
    :return: [TODO:description]
    """
    valid = True
    status_msg_long_lst = list()
    if type(ap.tail) is not type(fact.object):
        status_msg = "Observed value type differs from expected type."\
                     f"\n Expected: {type(ap.tail)}"\
                     f"\n Observed: {type(fact.object)}"
        status_msg_long_lst.append(status_msg)
        valid = False

        logger.info(status_msg)
    if isinstance(ap.tail, IRIRef) and ap.tail != fact.object:
        status_msg = "Observed IRI value differs from expected IRI value."\
                     f"\n Expected: {ap.tail}"\
                     f"\n Observed: {fact}"
        status_msg_long_lst.append(status_msg)
        valid = False

        logger.info(status_msg)
    elif isinstance(ap.tail, Literal):
        dtype_observed = infer_datatype(fact.object)
        if dtype_observed != ap.tail.dtype:
            status_msg = "Observed Literal value data type differs from "\
                         "expected Literal value data type."\
                         f"\n Expected: {'Unknown' if ap.tail.dtype is
                                         None else ap.tail.dtype}"\
                         f"\n Observed: {'Unknown' if dtype_observed is
                                         None else dtype_observed}"
            status_msg_long_lst.append(status_msg)
            valid = False

            logger.info(status_msg)
        if fact.object != ap.tail:
            status_msg = "Observed Literal value differs from expected "\
                         "Literal value."\
                         f"\n Expected: {ap.tail}"\
                         f"\n Observed: {fact}"
            status_msg_long_lst.append(status_msg)
            valid = False

            logger.info(status_msg)

    return valid, status_msg_long_lst


def validate_graph_structure(gPattern: GraphPattern,
                             fact_ap_pairs: list[tuple], unmatched: set,
                             match_cwa: bool, match_exact: bool)\
        -> tuple[list[str], list[str]]:
    """ Validate the structure of the observed state graph, by checking
        possible mapped and unmapped sub-structures.

    :param gPattern: [TODO:description]
    :param graph: [TODO:description]
    :param fact_ap_pairs: [TODO:description]
    :param unmatched: [TODO:description]
    :param owa: [TODO:description]
    :param exact: [TODO:description]
    :return: [TODO:description]
    """
    status_msg_lst = list()
    status_msg_long_lst = list()
    if len(fact_ap_pairs) < len(gPattern) and len(unmatched) <= 0:
        # incomplete mapping, =yet no unmatched facts remain
        status_msg_long = "Observed state graph contains less components "\
                          "than required by the associated graph "\
                          f"pattern: {len(fact_ap_pairs)} < "\
                          f"{len(gPattern)}"
        logger.info(status_msg_long)

        if match_exact:
            status_msg_lst.append("Exact Match Requirement Violated.")
            status_msg_long_lst.append(status_msg_long)

        if match_cwa:
            status_msg_lst.append("Closed-World Assumption Violated.")
            status_msg_long_lst.append(status_msg_long)

    elif len(fact_ap_pairs) == len(gPattern) and len(unmatched) > 0:
        # all known facts are matched, yet more are observed
        status_msg_long = "Observed state graph contains more components "\
                          "than required by the associated graph "\
                          f"pattern: {len(fact_ap_pairs)} > "\
                          f"{len(gPattern)}"
        logger.info(status_msg_long)

        if match_exact:
            # do not allow updates to structure
            status_msg_lst.append("Exact Match Requirement Violated.")
            status_msg_long_lst.append(status_msg_long)

    return status_msg_lst, status_msg_long_lst


class ValidationReport():
    class StatusCode(Enum):
        FAILED = 1  # p-value under critical significance level
        SUSPICIOUS = 2  # p-value under non-critical significance level
        PASSED = 3  # p-value above non-critical significance level
        ERROR = 4  # error during evaluation

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

    def __hash__(self):
        return hash(str(self.pattern)
                    + str(self.graph)
                    + ', '.join(self.status_msg_long)
                    + str(self.timestamp))

    def __str__(self):
        return '\n'.join(self.status_msg_long)

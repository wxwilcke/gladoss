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

from gladoss.core.multimodal.datatypes import (XSD_CONTINUOUS, XSD_DISCRETE,
                                               cast_literal, infer_datatype)
from gladoss.core.pattern import AssertionPattern, GraphPattern
from gladoss.core.stats import (ContinuousDistribution, DiscreteDistribution,
                                Distribution, test_statistic_continuous,
                                test_statistic_discrete,
                                two_sample_hypothesis_test)
from gladoss.core.utils import match_facts_to_patterns


logger = logging.getLogger(__name__)


def validate_state_graph(gPattern: GraphPattern,
                         facts: Collection[Statement],
                         config: SimpleNamespace)\
        -> Optional[ValidationReport]:
    # find pairs of facts and associated assertion patterns
    fact_ap_pairs, unmatched = match_facts_to_patterns(
            facts,
            gPattern.pattern)

    if len(fact_ap_pairs) < len(gPattern) and len(unmatched) > 0:
        logger.warning("Unable to map all components of the observed "
                       "state graph to their association substructure "
                       "patterns. Skipping further evaluation. "
                       f"Unmapped components: {', '.join(unmatched)}.")

        return None  # skip further evaluation

    if config.evaluate_structure:
        # validate the structure of the state graph
        report = validate_graph_structure(gPattern, facts, fact_ap_pairs,
                                          unmatched, config.match_cwa,
                                          config.match_exact)
        if report is not None:
            return report

    # TODO: skip is dist size is too low


def validate_graph_data(rng: np.random.Generator,
                        fact: Statement, ap: AssertionPattern,
                        samplesize: int, interruption: int)\
        -> tuple[bool, str]:
    status_msg = ""
    if isinstance(ap.tail, Resource):
        if type(ap.tail) is not type(fact.object):
            status_msg = "Observed value type differs from expected type."\
                         f"\n Expected: {type(ap.tail)}"\
                         f"\n Observed: {type(fact.object)}"

            return False, status_msg
        elif isinstance(ap.tail, IRIRef) and ap.tail != fact.object:
            status_msg = "Observed IRI value differs from expected IRI value."\
                         f"\n Expected: {ap.tail}"\
                         f"\n Observed: {fact}"

            return False, status_msg
        elif isinstance(ap.tail, Literal):
            dtype_observed = infer_datatype(fact.object)
            if infer_datatype(fact.object) != ap.tail.dtype:
                status_msg = "Observed Literal value data type differs from "\
                             "expected Literal value data type."\
                             f"\n Expected: {'Unknown' if ap.tail.dtype is
                                             None else ap.tail.dtype}"\
                             f"\n Observed: {'Unknown' if dtype_observed is
                                             None else dtype_observed}"

                return False, status_msg
            elif fact.object != ap.tail:
                status_msg = "Observed Literal value differs from expected "\
                             "Literal value."\
                             f"\n Expected: {ap.tail}"\
                             f"\n Observed: {fact}"

                return False, status_msg

        return True, "Observed value matches expected value."

    if isinstance(ap.tail, Distribution):
        value_new = None
        test_statistic = None
        if isinstance(ap.tail, DiscreteDistribution):
            test_statistic = test_statistic_discrete
            if isinstance(fact.object, Literal):
                dtype_observed = infer_datatype(fact.object)
                if dtype_observed not in XSD_DISCRETE:
                    status_msg = "Observed value type does not fit to "\
                                 "expected distribution type."\
                                 f"\n Expected: {type(ap.tail)}"\
                                 f"\n Observed: {type(fact.object)}"

                    return False, status_msg
                if ap.tail.dtype is not None\
                        and dtype_observed != ap.tail.dtype:
                    status_msg = "Observed value data type does not fit to "\
                                 "expected distribution data type."\
                                 f"\n Expected: {'Unknown' if ap.tail.dtype is
                                                 None else ap.tail.dtype}"\
                                 f"\n Observed: {'Unknown' if dtype_observed is
                                                 None else dtype_observed}"

                    return False, status_msg

                # cast value to appropriate format
                value_new = cast_literal(dtype_observed, fact.object.value)
            else:  # IRI
                value_new = fact.object
        if isinstance(ap.tail, ContinuousDistribution):
            test_statistic = test_statistic_continuous
            if not isinstance(fact.object, Literal):
                status_msg = "Observed value type does not fit to "\
                             "expected distribution type."\
                             f"\n Expected: {type(ap.tail)}"\
                             f"\n Observed: {type(fact.object)}"

                return False, status_msg

            dtype_observed = infer_datatype(fact.object)
            if dtype_observed not in XSD_CONTINUOUS:
                status_msg = "Observed value type does not fit to "\
                             "expected distribution type."\
                             f"\n Expected: {type(ap.tail)}"\
                             f"\n Observed: {type(fact.object)}"

                return False, status_msg
            if ap.tail.dtype is not None\
                    and dtype_observed != ap.tail.dtype:
                status_msg = "Observed value data type does not fit to "\
                             "expected distribution data type."\
                             f"\n Expected: {'Unknown' if ap.tail.dtype is
                                             None else ap.tail.dtype}"\
                             f"\n Observed: {'Unknown' if dtype_observed is
                                             None else dtype_observed}"

                return False, status_msg

            # cast value to appropriate format
            value_new = cast_literal(dtype_observed, fact.object.value)

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



def validate_graph_structure(gPattern: GraphPattern,
                             facts: Collection[Statement],
                             fact_ap_pairs: list[tuple], unmatched: set,
                             match_cwa: bool, match_exact: bool)\
        -> Optional[ValidationReport]:
    """ Validate the structure of the observed state graph, by checking
        possible mapped and unmapped sub-structures.

    :param gPattern: [TODO:description]
    :param facts: [TODO:description]
    :param fact_ap_pairs: [TODO:description]
    :param unmatched: [TODO:description]
    :param owa: [TODO:description]
    :param exact: [TODO:description]
    :return: [TODO:description]
    """
    report = None
    if len(fact_ap_pairs) < len(gPattern) and len(unmatched) <= 0:
        # incomplete mapping, =yet no unmatched facts remain
        status_msg_long = "Observed state graph contains less components "\
                          "than required by the associated graph "\
                          f"pattern: {len(fact_ap_pairs)} < "\
                          f"{len(gPattern)}"
        logger.info(status_msg_long)
        if match_cwa or match_exact:
            status_msg = "Closed-World Assumption Violated."
            status_code = ValidationReport.StatusCode.FAILED

            report = ValidationReport(pattern=gPattern,
                                      facts=facts,
                                      timestamp=datetime.now(),
                                      status_code=status_code,
                                      status_msg=status_msg,
                                      status_msg_long=status_msg_long)

            return report

    if len(fact_ap_pairs) == len(gPattern) and len(unmatched) > 0:
        # all known facts are matched, yet more are observed
        status_msg_long = "Observed state graph contains more components "\
                          "than required by the associated graph "\
                          f"pattern: {len(fact_ap_pairs)} > "\
                          f"{len(gPattern)}"
        logger.info(status_msg_long)
        if match_exact:
            # do not allow updates to structure
            status_msg = "Exact Match Requirement Violated."
            status_code = ValidationReport.StatusCode.FAILED

            report = ValidationReport(pattern=gPattern,
                                      facts=facts,
                                      timestamp=datetime.now(),
                                      status_code=status_code,
                                      status_msg=status_msg,
                                      status_msg_long=status_msg_long)

            return report

    return report


class ValidationReport():
    class StatusCode(Enum):
        PASSED = 0
        FAILED_AT_CRITICAL_LEVEL = 1
        FAILED_AT_SUSPICIOUS_LEVEL = 3
        RUNTIME_ERROR = 5

    def __init__(self, pattern: GraphPattern, facts: Collection[Statement],
                 timestamp: datetime, status_code: StatusCode,
                 status_msg: str, status_msg_long: str) -> None:
        self.pattern = pattern
        self.facts = facts
        self.timestamp = timestamp
        self.status_code = status_code
        self.status_msg = status_msg
        self.status_msg_long = status_msg_long

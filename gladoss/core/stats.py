#! /usr/bin/env python

from __future__ import annotations
from collections import Counter
from enum import Enum, auto
import logging
from typing import Any, Optional
import sys

import numpy as np
from rdf.terms import IRIRef, Literal, Resource
from rdf.namespaces import RDFS

from gladoss.core.multimodal.datatypes import (XSD_CONTINUOUS, XSD_DISCRETE,
                                               cast_literal, infer_datatype)


logger = logging.getLogger(__name__)

# TODO: change each dist to trend analysis?


class Distribution():
    def __init__(self, rng: np.random.Generator,
                 decay: int = -1,
                 dtype: Optional[IRIRef] = None) -> None:
        """ Statistical distribution over samples of a specific
            (data) type. Samples are kept until they decay (a
            negative value disables this feature) and the last
            n samples are used to estimate the distribution
            parameters (a negative value implies the use of all
            past samples). An optional (data) type can be given
            to validate new samples (a distribution of IRIs
            has rdfs:Resource as datatype).

        :param decay: time until seen sample is forgotten
        """
        self._rng = rng
        self.decay = decay
        self.dtype = dtype

        self._t = 0  # time steps since initialisation
        self.samples = Counter()  # keep track of samples and their frequency
        self._decay_tracker = dict()  # keep track of decay

    def addSample(self, sample: str | float) -> None:
        """ Add a single sample to the distribution and push
            the time forward by steps.

        :param sample: the sample to add
        """
        # add sample to distribution or increment count
        if sample not in self.samples.keys():
            self.samples[sample] = 0
        self.samples[sample] += 1

        # schedule future decay of sample
        t_decay = (self._t + self.decay) % sys.maxsize
        self._decay_tracker[t_decay] = [sample]

        # increment time
        self._forward()

    @staticmethod
    def create_from(rng: np.random.Generator,
                    resource: Resource,
                    decay: int, resolution: int) -> Distribution:
        """ Create new distribution from the given resource. Infers
            datatype from semantic annotations or, if unavailable,
            using python heuristics. Once created, the given resource
            is added to the distribution.

        :param rng: [TODO:description]
        :param resource: [TODO:description]
        :param sample_size: [TODO:description]
        :param decay: [TODO:description]
        :return: [TODO:description]
        :raises NotImplementedError: [TODO:description]
        """
        if isinstance(resource, Literal):
            # determine datatype
            dtype = infer_datatype(resource)

            # creae new distribution
            if dtype in XSD_CONTINUOUS:
                dist = ContinuousDistribution(rng=rng,
                                              resolution=resolution,
                                              decay=decay,
                                              dtype=dtype)
            elif dtype in XSD_DISCRETE:
                dist = DiscreteDistribution(rng=rng,
                                            decay=decay,
                                            dtype=dtype)
            else:
                raise NotImplementedError(f"Datatype not supported: {dtype}")

            # cast value to appropriate format and add to distribution
            value = cast_literal(dtype, resource.value)
            dist.addSample(value)  # type: ignore

        else:  # IRIRef
            # create a new distribution and add values
            dist = DiscreteDistribution(rng=rng,
                                        decay=decay,
                                        dtype=RDFS+'Resource')

            dist.addSample(resource)

        return dist

    @property
    def data(self) -> list:
        """ Return all samples in the current population. A deterministic
            ordering between calls is not guaranteed.

        :param self [TODO:type]: [TODO:description]
        :return: [TODO:description]
        """
        return list(self.samples.elements())

    def lastn(self, n: int) -> list:
        """ Return the *n* most recently observed samples, ordered from
            oldest to most recent.

        :param n: [TODO:description]
        :return: [TODO:description]
        """
        return self.data[-n:]

    def _forward(self):
        """ Update the distribution by a single epoch. Update the
            decay tracker.
        """
        self._t += 1
        if self._t == sys.maxsize:
            self._t = 0

        if self.decay > 0:
            self._decay()

    def _decay(self) -> None:
        """ Forget about samples that have exceeded their decay period.
            This operation subtracts one occurrence from the frequency of
            the samples slated for decay at this epoch. Removes the entry
            afterwards to clean up the data structure.
        """
        if self._t in self._decay_tracker.keys():
            for sample in self._decay_tracker[self._t]:
                if sample in self.samples.keys():
                    # decrease sample count
                    self.samples[sample] -= 1

                if self.samples[sample] <= 0:
                    # remove sample from index
                    del self.samples[sample]

                # clean up decay tracker
                del self._decay_tracker[self._t]

    def __repr__(self) -> str:
        return ", ".join(sorted(self.data))

    def __hash__(self) -> int:
        return hash(repr(self))

    def __eq__(self, other: Any) -> bool:
        return type(self) is type(other) and repr(self) == repr(other)

    def __lt__(self, other: Distribution) -> bool:
        return repr(self) < repr(other)


class ContinuousDistribution(Distribution):
    def __init__(self, rng: np.random.Generator, decay: int = -1,
                 resolution: int = -1,
                 dtype: Optional[IRIRef] = None) -> None:
        """ Continuous distribution over Real numbers. A
            resolution above zero will truncate the samples
            to that number of significant figures.

        :param decay: time until seen sample is forgotten
        :param resolution: number of significant figures
        """
        super().__init__(rng, decay, dtype)
        self.resolution = resolution

    def addSample(self, sample: float) -> None:
        """ Add a single sample to the distribution.

        :param sample: a Real number
        """
        sample = self._truncate(sample)

        return super().addSample(sample)

    def _truncate(self, sample: float) -> float:
        """ Truncate sample to its significant numbers.

        :param sample: the real number to truncate
        :return: the original or truncated real number
        """
        if self.resolution <= 0:
            return sample

        return float(f"%.{self.resolution}g" % sample)

    def __deepcopy__(self, memo) -> ContinuousDistribution:
        """ Create a copy which deepcopies only the dynamic
            elements whereas it creates references to static
            and immutable objects.

        :param memo [TODO:type]: [TODO:description]
        :return: [TODO:description]
        """
        dist = ContinuousDistribution(rng=self._rng, decay=self.decay,
                                      resolution=self.resolution)

        dist.samples = Counter(self.samples.elements())

        dist._t = self._t
        dist._decay_tracker = {k: v for k, v in self._decay_tracker.items()}

        return dist

    def __str__(self) -> str:
        pass


class DiscreteDistribution(Distribution):
    def __init__(self, rng: np.random.Generator,
                 decay: int = -1, dtype: Optional[IRIRef] = None) -> None:
        """ Discrete distribution

        :param decay: time until seen sample is forgotten
        """
        super().__init__(rng, decay, dtype)

    def addSample(self, sample: str) -> None:
        """ Add a single sample to the distribution.

        :param sample: a string value
        """
        sample = sample.strip().lower()  # standardize sample

        return super().addSample(sample)

    def __deepcopy__(self, memo) -> DiscreteDistribution:
        """ Create a copy which deepcopies only the dynamic
            elements whereas it creates references to static
            and immutable objects.

        :param memo [TODO:type]: [TODO:description]
        :return: [TODO:description]
        """
        dist = DiscreteDistribution(rng=self._rng,
                                    decay=self.decay)

        dist.samples = Counter(self.samples.elements())

        dist._t = self._t
        dist._decay_tracker = {k: v for k, v in self._decay_tracker.items()}

        return dist

    def __str__(self) -> str:
        pass


def test_statistic_discrete(samples: np.ndarray, samples_idx: dict)\
        -> np.ndarray:
    """ Compute proportions for each unique sample in the data.
        Adds NaN for elements that are present in the population
        (and thus in the provided sample index) yet which are absent
        from the provided sample. The output is ordered as specified
        in the provided sample index.

        Note that other discrete test statistics (eg mode and frequency)
        are omitted since these co-vary with proportion and thus not
        tell us anything more.

    :param samples: [TODO:description]
    :param samples_idx: [TODO:description]
    :return: [TODO:description]
    """
    counter = Counter([v for v in samples])

    n = len(samples)
    proportions = np.array([np.nan] * len(samples_idx), dtype=float)
    for k in counter.keys():
        p = counter[k] / n  # proportion in sample

        proportions[samples_idx[k]] = p

    return proportions


def test_statistic_continuous(samples: np.ndarray, samples_idx: dict) ->\
        tuple[np.floating, np.floating, np.floating]:
    """ Compute the mean, median, and sample standard deviation
        for the provided sample.

    :param samples: [TODO:description]
    :return: [TODO:description]
    """
    mean = np.mean(samples)
    stdev = np.std(samples, mean=mean, ddof=1)  # sample stdev
    median = np.median(samples)

    return mean, stdev, median


class HypothesisTest(Enum):
    REJECT_H0 = auto()
    NOT_REJECT_H0 = auto()


def two_sample_hypothesis_test(rng: np.random.Generator,
                               sample_a: np.ndarray,
                               sample_b: np.ndarray,
                               test_statistic_func,
                               num_samples: int,
                               num_resamples: int = 1000,
                               alpha: float = 0.05) -> HypothesisTest:
    """ Compute the p-values for a two-sample hypothesis test via
        the bootstrap method, and return a majority vote over the
        test statistics that either support rejecting or not
        rejecting the null hypothesis at the provided significance
        level alpha.

    :param rng: [TODO:description]
    :param sample_a: [TODO:description]
    :param sample_b: [TODO:description]
    :param test_statistic_func [TODO:type]: [TODO:description]
    :param num_samples: [TODO:description]
    :param num_resamples: [TODO:description]
    :param alpha: [TODO:description]
    :return: [TODO:description]
    """
    # compute p-values via bootstrap method
    p_values = two_sample_bootstrap_hypothesis_test(rng, sample_a, sample_b,
                                                    test_statistic_func,
                                                    num_samples, num_resamples)

    # majority vote over test statistics
    reject_h0_lst = (p_values < alpha/2) | (p_values > 1 - alpha/2)
    reject_h0 = (reject_h0_lst.sum() / len(reject_h0_lst)) > 0.5
    # FIXME: also eval at a=.10? to see suspicious behaviour?

    return HypothesisTest.REJECT_H0 if reject_h0\
        else HypothesisTest.NOT_REJECT_H0


def two_sample_bootstrap_hypothesis_test(rng: np.random.Generator,
                                         sample_a: np.ndarray,
                                         sample_b: np.ndarray,
                                         test_statistic_func,
                                         num_samples: int,
                                         num_resamples: int) -> np.ndarray:
    """ Compute the p-values for a two-sample hypothesis test. The Bootstrap
        method is used to generate an emperical discrete distribution as an
        approximation of the actual distribution, from which the test
        statistic t is computed for both samples A and B. The null hypothesis
        H0 is P(t_A < t_B) = 0.5 whereas the alternative hypothesis Ha is
        P(t_A < t_B) != 0.5. For H0 to be rejected, either 'p < a/2' or
        'p > 1 - a/2', with a the significance level.

        This is a non-parametric test that makes no assumption about the
        underlying distribution. Samples should be i.i.d., which is not
        necessarily the case for time series data (eg temperature), but
        which can still be assumed to hold to an extent in those cases when
        the temporal resolution is sufficiently low.

        Accomodate for NaN values by omitting these entries from affecting
        the computed p-values, by reducing the denominator by an equal
        amount. This only concerns discrete data.

    :param rng: [TODO:description]
    :param sample_a: [TODO:description]
    :param sample_b: [TODO:description]
    :param test_statistic_func [TODO:type]: [TODO:description]
    :param num_samples: [TODO:description]
    :param num_resamples: [TODO:description]
    :return: [TODO:description]
    """
    # TODO: reuse results from previous calls if provided?

    # generate index for unique elements
    samples_uniq = np.union1d(np.unique(sample_a), np.unique(sample_b))
    samples_idx = {sample: i for i, sample in enumerate(samples_uniq)}

    # bootstrap method
    results_a, results_b = list(), list()
    for samples, results in [(sample_a, results_a),
                             (sample_b, results_b)]:
        for _ in range(num_resamples):
            # draw random samples with replacement
            sample_hat = rng.choice(samples, size=num_samples, replace=True)

            # compute test statistics and store the results
            results.append(test_statistic_func(sample_hat, samples_idx))

    # convert to <num resamples> x <num test statistics> arrays
    results_a = np.array(results_a)
    results_b = np.array(results_b)

    # Count number of invalid comparisons (where either or both are NaN)
    # for each test statistic separately.
    num_nan = np.count_nonzero(np.isnan(results_a) | np.isnan(results_b),
                               axis=0)

    # Test hypothesis and aAggregate results per test statistic.
    # Remove zeros if present (caused by values in A yet not in B)
    results = (results_a < results_b).sum(axis=0)
    nonzero_mask = results.nonzero()
    results = results[nonzero_mask]  # omit zeros

    # Subtract invalid comparisons per statistic from the denominator since
    # these should not impact the final proportion.
    num_tests = results_a.shape[1]  # number of test statistics per row
    num_resamples_adjusted = np.tile(num_resamples, reps=num_tests) - num_nan
    num_resamples_adjusted = num_resamples_adjusted[nonzero_mask]

    # Compute p-values by counting the proportion of values that support H0.
    p_values = results / num_resamples_adjusted

    return p_values

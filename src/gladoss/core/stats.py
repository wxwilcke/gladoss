#! /usr/bin/env python

from __future__ import annotations
from collections import Counter
from enum import Enum, auto
import logging
from typing import Any, Optional
import sys

import numpy as np
from rdf.terms import IRIRef, Literal, Resource
from rdf.namespaces import XSD

from gladoss.core.multimodal.datatypes import (XSD_CONTINUOUS, XSD_DISCRETE,
                                               cast_literal, infer_datatype)


logger = logging.getLogger(__name__)

# TODO: consider swapping distributions for LSTMs with OCSVMs


class Distribution():
    def __init__(self,
                 decay: int = -1,
                 dtype: Optional[IRIRef] = None,
                 lang: Optional[str] = None) -> None:
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
        self.decay = decay
        self.dtype = dtype
        self.lang = lang

        self._t = 0  # time steps since initialisation
        self.samples = Counter()  # keep track of samples and their frequency
        self._decay_tracker = dict()  # keep track of decay

    def addSample(self, sample: str | float) -> None:
        """ Add a single sample to the distribution and push
            the time forward by one step.

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
    def create_from(resource: Resource,
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
            lang = resource.language

            # creae new distribution
            if dtype in XSD_CONTINUOUS:
                dist = ContinuousDistribution(resolution=resolution,
                                              decay=decay,
                                              dtype=dtype,
                                              lang=lang)
            elif dtype in XSD_DISCRETE:
                dist = DiscreteDistribution(decay=decay,
                                            dtype=dtype,
                                            lang=lang)
            else:
                raise NotImplementedError(f"Datatype not supported: {dtype}")

            # cast value to appropriate format and add to distribution
            value = cast_literal(dtype, resource)
            dist.addSample(value)  # type: ignore

        else:  # IRIRef
            # create a new distribution and add values
            # use xsd:anyURI as IRI type
            dist = DiscreteDistribution(decay=decay,
                                        dtype=XSD+'anyURI')

            dist.addSample(resource.value)

        return dist

    @property
    def num_samples(self) -> int:
        """ Return the total number of samples that make up the
            distribution.

        :param self [TODO:type]: [TODO:description]
        :return: [TODO:description]
        """
        return len(self.data)

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

    def fluidity(self) -> float:
        """ Measure of change in [0, 1] of the values that make up the
            distribution. A value of zero implies no change (a static value,
            eg a fixed IRI) whereas a value of one implies that no value is
            seen more than once (eg random categorical data or dynamic
            continuous values).

        :return: [TODO:description]
        """
        num_items = len(self.samples.keys())
        return 0. if num_items <= 1\
            else float(num_items / self.samples.total())

    def __repr__(self) -> str:
        return ", ".join([str(v) for v in sorted(self.data)])

    def __hash__(self) -> int:
        return hash(repr(self))

    def __eq__(self, other: Any) -> bool:
        return type(self) is type(other) and repr(self) == repr(other)

    def __lt__(self, other: Distribution) -> bool:
        return repr(self) < repr(other)

    def __str__(self) -> str:
        return '[' + ", ".join([str(v) for v in self.data]) + ']'


class ContinuousDistribution(Distribution):
    def __init__(self, decay: int = -1,
                 resolution: int = -1,
                 dtype: Optional[IRIRef] = None,
                 lang: Optional[str] = None) -> None:
        """ Continuous distribution over Real numbers. A
            resolution above zero will truncate the samples
            to that number of significant figures.

        :param decay: time until seen sample is forgotten
        :param resolution: number of significant figures
        """
        super().__init__(decay, dtype, lang)
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
        dist = ContinuousDistribution(decay=self.decay,
                                      resolution=self.resolution,
                                      dtype=self.dtype)

        dist.samples = Counter(self.samples.elements())

        dist._t = self._t
        dist._decay_tracker = {k: v for k, v in self._decay_tracker.items()}

        return dist


class DiscreteDistribution(Distribution):
    def __init__(self, decay: int = -1,
                 dtype: Optional[IRIRef] = None,
                 lang: Optional[str] = None) -> None:
        """ Discrete distribution

        :param decay: time until seen sample is forgotten
        """
        super().__init__(decay, dtype, lang)

    def addSample(self, sample: str | int) -> None:
        """ Add a single sample to the distribution.

        :param sample: a string value
        """
        if isinstance(sample, str):
            sample = sample.strip().lower()  # standardize sample

        return super().addSample(sample)

    def __deepcopy__(self, memo) -> DiscreteDistribution:
        """ Create a copy which deepcopies only the dynamic
            elements whereas it creates references to static
            and immutable objects.

        :param memo [TODO:type]: [TODO:description]
        :return: [TODO:description]
        """
        dist = DiscreteDistribution(decay=self.decay,
                                    dtype=self.dtype,
                                    lang=self.lang)

        dist.samples = Counter(self.samples.elements())

        dist._t = self._t
        dist._decay_tracker = {k: v for k, v in self._decay_tracker.items()}

        return dist


def test_statistic_discrete(samples: np.ndarray, samples_idx: dict)\
        -> tuple[np.ndarray]:
    """ Compute proportions for each unique sample in the data.
        Adds NaN for elements that are present in the population
        (and thus in the provided sample index) yet which are absent
        from the provided sample. The output is ordered as specified
        in the provided sample index.

        Note that other discrete test statistics (eg mode and frequency)
        are omitted since these co-vary with proportion and thus not
        tell us anything more.

    :param samples: Samples to compute test statistics for
    :param samples_idx: Map between values and index used to align computed
                        test statistics across sample sets
    :return: [TODO:description]
    """
    counter = Counter([v for v in samples])

    n = len(samples)
    proportions = np.array([np.nan] * len(samples_idx), dtype=float)
    for k in counter.keys():
        p = counter[k] / n  # proportion in sample

        proportions[samples_idx[k]] = p

    return (proportions,)


def test_statistic_continuous(samples: np.ndarray, samples_idx: dict) ->\
        tuple[np.floating, np.floating, np.floating]:
    """ Compute the mean, median, and sample standard deviation
        for the provided sample.

        Note that the sample indices are not used by this function yet
        are asked to provide a uniform interface for all test statistics.

    :param samples: Samples to compute test statistics for
    :param samples_idx: Map between values and index used to align computed
                        test statistics across sample sets
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
                               alpha_critical: float = 0.05,
                               alpha_suspicious: float = 0.10)\
        -> list[HypothesisTest]:
    """ Compute the p-values for a two-sample hypothesis test via
        the bootstrap method, and return a majority vote over the
        test statistics that either support rejecting or not
        rejecting the null hypothesis at the provided significance
        level alpha.

    :param rng: NumPy random number generator
    :param sample_a: 1D NDArray with samples
    :param sample_b: 1D NDArray with samples
    :param test_statistic_func: Function to compute test statistics
    :param num_samples: Number of samples to draw from provided sample lists
    :param num_resamples: Number of times to recompute test statistics
    :param alpha_critical: Critical significance level
    :param alpha_suspicious: Suspicious significance level
    :return: List with outcome of hypothesis tests for critical and suspicious
             significance levels (in that order)
    """
    # compute p-values via bootstrap method
    p_values = two_sample_bootstrap_hypothesis_test(rng, sample_a, sample_b,
                                                    test_statistic_func,
                                                    num_samples, num_resamples)

    # majority vote over test statistics for all significance levels
    levels = [alpha_critical, alpha_suspicious]
    outcomes = [HypothesisTest.NOT_REJECT_H0] * len(levels)
    for i in range(len(levels)):
        alpha = levels[i]

        # two-sided significance test
        reject_h0_lst = (p_values < alpha/2) | (p_values > 1 - alpha/2)
        if (reject_h0_lst.sum() / len(reject_h0_lst)) > 0.5:
            # majority vote
            outcomes[i] = HypothesisTest.REJECT_H0

    return outcomes


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
        'p > 1 - a/2', with 'a' the significance level.

        This is a non-parametric test that makes no assumption about the
        underlying distribution. Samples should be i.i.d., which is not
        necessarily the case for time series data (eg temperature), but
        which can still be assumed to hold to an extent in those cases when
        the temporal resolution is sufficiently low.

        Accomodate for NaN values by omitting these entries from affecting
        the computed p-values, by reducing the denominator by an equal
        amount. This only concerns discrete data.

    :param rng: NumPy random number generator
    :param sample_a: 1D NDArray with samples
    :param sample_b: 1D NDArray with samples
    :param test_statistic_func: Function to compute test statistics
    :param num_samples: Number of samples to draw from provided sample lists
    :param num_resamples: Number of times to recompute test statistics
    :return: 1D NDArray with p-value per test statistic
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

    # Test hypothesis and aggregate results per test statistic.
    # Comparisons between NaN values (in either or both samples) resolve
    # to False
    results = (results_a < results_b).sum(axis=0)

    # Subtract invalid comparisons per statistic from the denominator since
    # these should not impact the final proportion.
    num_tests = results_a.shape[1]  # number of test statistics per row
    num_resamples_adjusted = np.tile(num_resamples, reps=num_tests) - num_nan

    # Compute p-values by counting the proportion of values that support H0.
    p_values = results / num_resamples_adjusted

    return p_values


def nonparametric_prediction_interval_range(n: int,
                                            p: float)\
        -> tuple[float | int, float | int]:
    """ Compute range of non-parametric prediction interval (l, u] for a
        population of size N and probability P. By employing order statistics,
        this procedure assumes that the probability that a value X is covered
        by the interval (S(l), S(u)] is at least (u - l) / (|S| + 1), with
        S(i) the i-th value in the population sample sorted in ascending order:

        P(X in (S(l), S(u)]) >= (u - l)/(|S| + 1)

        The returned prediction interval range is symmetric (lower and upper
        bound) and zero-based.

    :param n: [TODO:description]
    :param p: [TODO:description]
    :return: [TODO:description]
    """
    assert n >= 100

    # number of observations within the interval
    coverage = (n + 1) * p

    # index of lower and upper prediction intervals
    # subtract 1 for 0-based index
    pi_lower_i = max(0, (((n + 1) - coverage) / 2) - 1)
    pi_upper_i = min((n - pi_lower_i) - 1, n - 1)

    return pi_lower_i, pi_upper_i


def nonparametric_prediction_interval(population: np.ndarray,
                                      p: float)\
        -> tuple[float, float]:
    """ Compute non-parametric prediction interval (l, u] for a given
        population sample and probability P. For intervals that fall
        between two values a weigted average is taken.

        This is a non-parametric test that makes no assumption about the
        underlying distribution. Samples should be i.i.d., which is not
        necessarily the case for time series data (eg temperature), but
        which can still be assumed to hold to an extent in those cases when
        the temporal resolution is sufficiently low.

    :param population: [TODO:description]
    :param alpha: [TODO:description]
    :return: [TODO:description]
    """
    def predicate_interval(sample: np.ndarray,
                           pi_i: int | float) -> float:
        weight = pi_i % 1
        if weight > 0:
            # weigted average of the values from two adjacent points
            pi_bound_low = sample[int(np.floor(pi_i))]
            pi_bound_high = sample[int(np.ceil(pi_i))]

            diff_weighted = (pi_bound_high - pi_bound_low) * weight

            pi = pi_bound_low + diff_weighted
        else:
            # exact index
            pi = sample[int(pi_i)]

        return pi

    # order ascendingly
    population_sorted = np.sort(population)

    # compute range of prediction intervals
    n = len(population_sorted)
    pi_lower_i, pi_upper_i = nonparametric_prediction_interval_range(n, p)

    # compute values associated with the range
    pi_lower = predicate_interval(population_sorted, pi_lower_i)
    pi_upper = predicate_interval(population_sorted, pi_upper_i)

    return (pi_lower.item(), pi_upper.item())

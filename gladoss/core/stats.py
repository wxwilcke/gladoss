#! /usr/bin/env python

from __future__ import annotations
from collections import Counter
import logging
from typing import Any, Optional
import sys

import numpy as np
import scipy as sp
from rdf.terms import IRIRef, Literal, Resource

from gladoss.core.multimodal.datatypes import (XSD_CONTINUOUS, XSD_DISCRETE,
                                               cast_literal, infer_datatype)


logger = logging.getLogger(__name__)


class Distribution():
    def __init__(self, rng: np.random.Generator,
                 samplesize: int = -1,
                 decay: int = -1,
                 dtype: Optional[IRIRef] = None) -> None:
        """ Statistical distribution over samples of a specific
            (data) type. Samples are kept until they decay (a
            negative value disables this feature) and the last
            n samples are used to estimate the distribution
            parameters (a negative value implies the use of all
            past samples). An optional (data) type can be given
            to validate new samples (a distribution of IRIs
            has None as datatype).

        :param decay: time until seen sample is forgotten
        """
        self._rng = rng
        self.samplesize = samplesize
        self.decay = decay
        self.dtype = dtype

        self._t = 0  # time steps since initialisation
        self.samples = Counter()  # keep track of samples and their frequency
        self._decay_tracker = dict()  # keep track of decay

    def addSample(self, sample: Any) -> None:
        """ Add a single sample to the distribution and push
            the time forward by steps.

        :param sample: the sample to add
        """
        # add sample to distribution or increment count
        if sample not in self.samples.keys():
            self.samples[sample] = 0
        self.samples[sample] += 1

        # schedule future decay of sample
        if self.decay > 0:
            t_decay = (self._t + self.decay) % sys.maxsize
            self._decay_tracker[t_decay] = [sample]

        # increment time
        self._forward()

    @staticmethod
    def create_from(rng: np.random.Generator,
                    resource: Resource, samplesize: int,
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
                                              samplesize=samplesize,
                                              resolution=resolution,
                                              decay=decay,
                                              dtype=dtype)
            elif dtype in XSD_DISCRETE:
                dist = DiscreteDistribution(rng=rng,
                                            samplesize=samplesize,
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
                                        samplesize=samplesize,
                                        decay=decay)

            dist.addSample(resource)

        return dist

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

    def fit(self) -> None:
        pass

    def __repr__(self) -> str:
        return ", ".join(sorted(list(self.samples.elements())))

    def __hash__(self) -> int:
        return hash(repr(self))

    def __eq__(self, other: Any) -> bool:
        return type(self) is type(other) and repr(self) == repr(other)

    def __lt__(self, other: Distribution) -> bool:
        return repr(self) < repr(other)


class ContinuousDistribution(Distribution):
    _N = "\N{MATHEMATICAL BOLD SCRIPT CAPITAL N}"

    def __init__(self, rng: np.random.Generator, decay: int = -1,
                 samplesize: int = 10, resolution: int = -1,
                 dtype: Optional[IRIRef] = None) -> None:
        """ Continuous distribution over Real numbers. A
            resolution above zero will truncate the samples
            to that number of significant figures.

        :param decay: time until seen sample is forgotten
        :param resolution: number of significant figures
        """
        super().__init__(rng, samplesize, decay, dtype)
        self.resolution = resolution

        # distribution parameters
        self.shape = tuple()
        self.loc = 0.  # mu
        self.scale = 0.  # sigma

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

    def fit(self) -> None:
        samples = np.array(list(self.samples.elements()))
        bs_result = sp.stats.bootstrap

#    def fit(self, num_samples: int = 100) -> None:
#        """ Compute distribution parameters for a randomly selected
#            subset of observed samples. Note that the results are
#            estimated using Maximum Likelihood Estimation.
#
#        :param num_samples: size of subset to compute estimate on.
#        """
#        # fit Gaussian
#        # (alt: test which dist?: https://stackoverflow.com/questions/6620471/fitting-empirical-distribution-to-theoretical-ones-with-scipy-python)
#        # branch off when multiple modes are found (using GMM)
#
#        if self.samples.total() < num_samples:
#            logger.warning("Number of observed samples is less than "
#                           "specified sample size. This can reduce "
#                           "the precision of the fitted distribution.")
#
#            if self.samples.total() <= 1:
#                logger.info("To few samples observed to fit distribution")
#                return
#
#        # random sample (with repetition) to fit distribution to
#        subsample = self._rng.choice(list(self.samples.elements()),
#                                     size=num_samples)
#
#        try:
#            # fit distribution
#            params = sp.stats.norm.fit(subsample)
#
#            # split parameter components
#            self.shape = params[:-2]
#            self.loc = params[-2]
#            self.scale = params[-1]
#        except Exception as e:
#            logger.warning(f"Experienced error when fitting distribution: {e}")
#            return
#
#    def prob(self, sample: float) -> float:
#        """ Return the probability P of observing the given sample s
#            given distribution parameters theta: P(s|theta). Since
#            we cannot compute a point on a continuous distribution,
#            we just check for left or right-tailed probabilities.
#
#        :param sample: a real number to compute the probability for
#        :return: the left or right-tailed probability
#        """
#        # TODO: include sample_size over time
#        # TODO: check if correct
#        p_ltail = 2 * sp.stats.norm.cdf(sample, loc=self.loc, scale=self.scale)
#        if sample < self.loc:
#            return p_ltail
#        elif sample > self.loc:
#            return 1 - p_ltail  # right tail
#        else:
#            return 1.0
#
#        # TODO: alternative idea: check if value falls outside .95 or .99
#        # confidence interval
#        # sp.stats.norm(mu, sigma).interval(CI_level)
#
#    def loglikelihood(self) -> float:
#        """ Return log likelihood L of the distribution parameters theta
#            given the observed samples S: L(theta|S)
#
#        :return: a goodness of fit measurement
#        """
#        return float(
#            np.sum(np.log(sp.stats.norm.pdf(list(self.samples.elements()),
#                                            self.loc,
#                                            self.scale))))

    def __deepcopy__(self, memo) -> ContinuousDistribution:
        """ Create a copy which deepcopies only the dynamic
            elements whereas it creates references to static
            and immutable objects.

        :param memo [TODO:type]: [TODO:description]
        :return: [TODO:description]
        """
        dist = ContinuousDistribution(rng=self._rng, decay=self.decay,
                                      samplesize=self.samplesize,
                                      resolution=self.resolution)

        dist.samples = Counter(self.samples.elements())

        dist._t = self._t
        dist._decay_tracker = {k: v for k, v in self._decay_tracker.items()}

        dist.shape = self.shape
        dist.loc = self.loc
        dist.scale = self.scale

        return dist

    def __str__(self) -> str:
        return f"{ContinuousDistribution._N}({self.loc}, "\
               + f"{self.scale}\N{SUPERSCRIPT TWO})"


class DiscreteDistribution(Distribution):
    def __init__(self, rng: np.random.Generator, samplesize: int = 10,
                 decay: int = -1, dtype: Optional[IRIRef] = None) -> None:
        """ Discrete distribution

        :param decay: time until seen sample is forgotten
        """
        super().__init__(rng, samplesize, decay, dtype)

        self.n = 0
        self.k = list()
        self.p = 0.

    def fit(self) -> None:
        # FIXME: check if needed (why not on the fly?)
        # fit multinomial distribution (k >=2, n >= 1)

        # FIXME: Placeholder
        self.n = self.samplesize
        self.k = self.samples
        self.p = [k/self.n for k in self.k]

    def prob(self, sample: Any) -> tuple[float, tuple[float, float]]:
        """ Compute probability P of observing sample s given
            distribution parameters theta and past W observed
            samples, with W the sample_size size.

        :param sample: [TODO:description]
        :return: [TODO:description]
        """
        if sample not in self.samples.keys():
            logger.info("Provides sample is out-of-distribution")
            return 0., (0., 0.)
        if self.samples.total() < self.samplesize:
            logger.info("sample_size size exceeds number of observed samples")

        # time window and corresponding samples TODO: disconnect from decay
        t_max = max(self._decay_tracker.keys())
        window = [t for t in self._decay_tracker.keys()
                  if t in range(t_max-self.samplesize, t_max+1)]
        samples = [v for t in window for v in self._decay_tracker[t]]

        # add observed sample
        samples.append(sample)

        # compute counts and probabilities
        samples_counter = Counter(samples)
        x, p = list(), list()
        sample_i = -1
        for i, (value, freq) in enumerate(samples_counter.items()):
            x.append(freq)
            p.append(freq/self.samplesize)

            if value == sample:
                # keep track of sample index
                sample_i = i

        # compute probability
        prob = sp.stats.multinomial.pmf(x=x, n=len(samples), p=p)
        # FIXME: idea: compare to ideal prob?

        # compute confidence interval - recast problem as computing
        # binomial CI of observing vs not observing sample
        binom_result = sp.stats.binomtest(k=x[sample_i],
                                          n=self.samplesize,
                                          p=p[sample_i])
        ci_lb, ci_hb = binom_result.proportion_ci(confidence_level=0.95,
                                                  method='wilsoncc')

        return prob, (ci_lb, ci_hb)

    def loglikelihood(self) -> float:
        """ Return log likelihood L of the distribution parameters theta
            given the observed samples S: L(theta|S)

        :return: a goodness of fit measurement
        """
        pass

    def __deepcopy__(self, memo) -> DiscreteDistribution:
        """ Create a copy which deepcopies only the dynamic
            elements whereas it creates references to static
            and immutable objects.

        :param memo [TODO:type]: [TODO:description]
        :return: [TODO:description]
        """
        dist = DiscreteDistribution(rng=self._rng,
                                    samplesize=self.samplesize,
                                    decay=self.decay)

        dist.samples = Counter(self.samples.elements())

        dist._t = self._t
        dist._decay_tracker = {k: v for k, v in self._decay_tracker.items()}

        # distribution-specific parameters
        dist.n = self.n
        dist.k = self.k
        dist.p = self.p

        return dist

    def __str__(self) -> str:
        pass

def test_statistic_discrete(samples: np.ndarray, **kwargs) -> np.ndarray:
    key_index = kwargs['key_index']
    counter = Counter([v for v in samples])

    n = len(samples)
    proportions = np.zeros(len(key_index), dtype=float)
    for k in counter.keys():
        p = counter[k] / n  # proportion in sample

        proportions[key_index[k]] = p  # TODO: as Python object

    return proportions

def test_statistic_continuous(samples: np.ndarray, **kwargs) -> np.ndarray:
    return np.mean(samples), np.median(samples), np.var(samples)

def two_sample_bootstrap_hypothesis_test(rng: np.random.Generator,
                                         sample_a: np.ndarray,
                                         sample_b: np.ndarray,
                                         test_statistic_func,
                                         num_samples: int,
                                         num_resamples: int,
                                         **kwargs) -> np.ndarray:
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

    :param rng: [TODO:description]
    :param sample_a: [TODO:description]
    :param sample_b: [TODO:description]
    :param test_statistic_func [TODO:type]: [TODO:description]
    :param num_samples: [TODO:description]
    :param num_resamples: [TODO:description]
    :return: [TODO:description]
    """
    results_a, results_b = list(), list()
    for samples, results in [(sample_a, results_a),
                             (sample_b, results_b)]:
        for _ in range(num_resamples):
            # draw random samples with replacement
            sample_hat = rng.choice(samples, size=num_samples, replace=True)

            # compute test statistics and store the results
            results.append(test_statistic_func(sample_hat, **kwargs))

    # compute p-values
    results = np.array(results_a) < np.array(results_b)
    results = results.sum(axis=0) / num_resamples

    return results

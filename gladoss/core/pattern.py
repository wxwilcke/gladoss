#! /usr/bin/env python

from __future__ import annotations
from collections import Counter
from datetime import datetime
from enum import Enum, auto
import logging
from queue import Queue
import sys
from typing import Any, Dict, Iterator, List, Optional, Set, Union

import numpy as np
import scipy as sp

from rdf.graph import Statement
from rdf.terms import IRIRef, Literal, Resource


logger = logging.getLogger(__name__)


class IRIRefWrapper(IRIRef):
    """ IRIRef Wrapper class

    A wrapper around a IRIRef, but which stores additional semantic
    information (eg, the class or superproperty)
    """

    def __init__(self, resource: IRIRef,
                 parent: Optional[IRIRef] = None) -> None:
        """ Initialize and instance of this class

        :params resource: the IRI
        :returns: None
        """
        super().__init__(resource)

        self.parent = parent

    def __repr__(self) -> str:
        s = super().__str__()
        if self.parent is not None:
            s += f" [{str(self.parent)}]"

        return s

    def __hash__(self) -> int:
        return hash(self.__repr__())

    def __eq__(self, other: Any) -> bool:
        if type(other) is not IRIRefWrapper:
            return False

        return (self.parent is None or self.parent == other.parent)\
            and self.value == other.value

    def __lt__(self, other: IRIRefWrapper) -> bool:
        return (self.parent is not None and self.parent < other.parent)\
                or ((self.parent is None or self.parent == other.parent)
                    and self.value < other.value)


class Distribution():
    def __init__(self, rng: np.random.Generator,
                 sample_size: int = -1,
                 decay: int = -1) -> None:
        """ Distribution over samples.

        :param decay: time until seen sample is forgotten
        """
        self._rng = rng
        self.samples = Counter()
        self.sample_size = sample_size
        self.decay = decay
        self._t = 0
        self._decay_tracker = dict()

    def addSample(self, sample: Any) -> None:
        """ Add a single sample to the distribution.

        :param sample: the sample to add
        """
        # add sample to distribution or increment count
        if sample not in self.samples.keys():
            self.samples[sample] = 0
        self.samples[sample] += 1

        # schedule future decay of sample
        if self.decay > 0:
            t_decay = (self._t + self.decay) % sys.maxsize
            self._decay_tracker[t_decay] = sample

        # increment time
        self.update()

    def update(self):
        """ Update the distribution by a single time step.
        """
        self._t += 1
        if self._t == sys.maxsize:
            self._t = 0

        if self.decay > 0:
            self._decay()

    def _decay(self) -> None:
        """ Forget about samples that have exceeded their decay period.
        """
        if self._t in self._decay_tracker.keys():
            sample = self._decay_tracker[self._t]
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
        return ", ".join(s for s in self.samples.elements())

    def __hash__(self) -> int:
        return hash(repr(self))

    def __eq__(self, other: Any) -> bool:
        return type(self) is type(other) and repr(self) == repr(other)

    def __lt__(self, other: Distribution) -> bool:
        return repr(self) < repr(other)


class ContinuousDistribution(Distribution):
    _N = "\N{MATHEMATICAL BOLD SCRIPT CAPITAL N}"

    def __init__(self, rng: np.random.Generator, decay: int = -1,
                 resolution: int = -1) -> None:
        """ Continuous distribution over Real numbers.

        :param decay: time until seen sample is forgotten
        :param resolution: number of significant figures
        """
        super().__init__(rng, decay)
        self.resolution = resolution

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

    def fit(self, num_samples: int = 100) -> None:
        """ Compute distribution parameters for a randomly selected
            subset of observed samples. Note that the results are
            estimated using Maximum Likelihood Estimation.

        :param num_samples: size of subset to compute estimate on.
        """
        # fit Gaussian
        # (alt: test which dist?: https://stackoverflow.com/questions/6620471/fitting-empirical-distribution-to-theoretical-ones-with-scipy-python)
        # branch off when multiple modes are found (using GMM)

        if self.samples.total() < num_samples:
            logger.warning("Number of observed samples is less than "
                           "specified sample size. This can reduce "
                           "the precision of the fitted distribution.")

            if self.samples.total() <= 1:
                logger.info("To few samples observed to fit distribution")
                return

        # random sample (with repetition) to fit distribution to
        subsample = self._rng.choice(list(self.samples.elements()),
                                     size=num_samples)

        try:
            # fit distribution
            params = sp.stats.norm.fit(subsample)
            
            # split parameter components
            self.shape = params[:-2]
            self.loc = params[-2]
            self.scale = params[-1]
        except Exception:
            return


    def prob(self, sample: float) -> float:
        """ Return the probability P of observing the given sample s
            given distribution parameters theta: P(s|theta). Since 
            we cannot compute a point on a continuous distribution,
            we just check for left or right-tailed probabilities.

        :param sample: a real number to compute the probability for
        :return: the left or right-tailed probability
        """
        # TODO: include sample_size?
        p_ltail = 2 * sp.stats.norm.cdf(sample, loc=self.loc, scale=self.scale)
        if sample < self.loc:
            return p_ltail
        elif sample > self.loc:
            return 1 - p_ltail  # right tail
        else:
            return 1.0

    def loglikelihood(self) -> float:
        """ Return log likelihood L of the distribution parameters theta
            given the observed samples S: L(theta|S)

        :return: a goodness of fit measurement
        """
        return float(
            np.sum(np.log(sp.stats.norm.pdf(list(self.samples.elements()),
                                                 self.loc,
                                                 self.scale))))
        

    def __str__(self) -> str:
        return f"{ContinuousDistribution._N}({self.loc}, "\
               + f"{self.scale}\N{SUPERSCRIPT TWO})"

    def __repr__(self) -> str:
        return self.__str__() + f" [decay: {self.decay}, "\
                                + f"res: {self.resolution}, "\
                                + f"n: {self.samples.total()}]"


class DiscreteDistribution(Distribution):
    def __init__(self, rng: np.random.Generator, sample_size: int = 10,
                 decay: int = -1) -> None:
        """ Discrete distribution

        :param decay: time until seen sample is forgotten
        """
        super().__init__(rng, sample_size, decay)

    def fit(self) -> None:
        # FIXME: check if needed (why not on the fly?)
        # fit multinomial distribution (k >=2, n >= 1)

        # FIXME: Placeholder
        self.n = self.sample_size
        self.k = self.samples
        self.p = [k/self.n for k in self.k]

    def prob(self, sample: float) -> float:
        """ Compute probability P of observing sample s given
            distribution parameters theta and past W observed
            samples, with W the sample_size size.

        :param sample: [TODO:description]
        :return: [TODO:description]
        """
        if sample not in self.samples.keys():
            logger.info("Provides sample is out-of-distribution")
            return 0.
        if self.samples.total() < self.sample_size:
            logger.info("sample_size size exceeds number of observed samples")

        observed_samples = ... # + sample
        observed_samples_prob = ...
        p = sp.stats.multinomial.pmf(x=observed_samples,
                                     n=self.sample_size + 1,
                                     p=observed_samples_prob)
        return p


class AssertionPattern():
    def __init__(self,  head: IRIRefWrapper | DiscreteDistribution,
                 relation: IRIRefWrapper,
                 tail: IRIRefWrapper | Literal | Distribution) -> None:
        """ Initialize a new AssertionPattern.

        :param head: the subject of an assertion or its distribution
        :param relation: the predicate of an assertion
        :param tail: the object of an assertion or its distribution
        """
        self.head = head
        self.relation = relation
        self.tail = tail

    def weak_match(self, other: Statement) -> bool:
        """ Check if a given Statement matches the pattern by comparing
            the types and values of their elements, with the exception
            of any distributions the pattern might have (which cannot
            occur in Statements).

        :param other: [TODO:description]
        :return: [TODO:description]
        """
        if type(other) is not Statement:
            return False

        if (isinstance(self.relation, IRIRef)
            and self.relation != other.relation)\
           or (isinstance(self.head, IRIRef)
               and self.head != other.head)\
           or ((isinstance(self.tail, IRIRef)
                or isinstance(self.tail, Literal))
               and self.tail != other.tail):
            return False

        return True

    def strong_match(self, other: Statement) -> bool:
        return self.__eq__(other)

    def __eq__(self, other: Any) -> bool:
        return str(self) == str(other)

    def __lt__(self, other: AssertionPattern) -> bool:
        return str(self) < str(other)

    def __hash__(self) -> int:
        """ Return unique hash for each assertion

        :rtype: int
        """
        return hash(str(self))

    def __str__(self) -> str:
        """ Return the string description of this assertion

        :returns: a description of this assertion
        :rtype: str
        """
        return "(" + ', '.join([str(self.head),
                                str(self.relation),
                                str(self.tail)]) + ")"


class GraphPattern():
    """ GraphPattern class

    Holds all assertions of a graph pattern and keeps
    track of the connections and distances (from the root) of these assertions.
    """

    def __init__(self, assertionPatterns: set[AssertionPattern],
                 anchors: set[Resource], decay: int = -1) -> None:
        self.pattern = assertionPatterns
        self.anchors = sorted(list(anchors))
        self.decay = decay
        self._t = 0
        self._decay_tracker = dict()


        # TODO: deal with changes in distribution
        #       assign index to assertions?
        self.assertions = Counter(self.pattern)

    def __len__(self) -> int:
        """ Return the number of assertions

        :rtype: int
        """
        return len(self.pattern)

    def update(self, g: set[Statement]) -> None:
        # create new pattern
        # for each different triple:
        #  create updated triple and add
        # add unchanged triples (efficient)
        # return updated pattern
        pass

    def __repr__(self) -> str:
        """ Return an internal string representation

        :rtype: str
        """
        return "GraphPattern [{}]".format(str(self))

    def __str__(self) -> str:
        """ Return a string representation

        :rtype: str
        """
        return "{" + "; ".join([str(assertion) for assertion in
                                sorted(self.pattern)]) + "}"

    def __hash__(self) -> int:
        return hash(str(self))


class PatternVault():
    def __init__(self) -> None:
        self._polytree = dict()

    def add(self, pattern: GraphPattern) -> None:
        key = ''.join(pattern.anchors)
        if key not in self._polytree.keys():
            self._polytree[key] = [(pattern, datetime.now())]

    def update(self, pattern: GraphPattern) -> None:
        key = ''.join(pattern.anchors)
        try:
            # TODO: compress old version
            # TODO: add timestamp?
            self._polytree[key].append((pattern, datetime.now()))
        except KeyError:
            return

    def create_associated_pattern(self, fact_set: set[Statement],
                                  anchor_set: set[Resource]) -> GraphPattern:
        # TODO: create pattern
        
        pattern = ...
        self.add(pattern)

    def find_associated_pattern(self, anchor_set: set[Resource])\
            -> GraphPattern | None:
        """ Find and return most recent associated pattern.

        :param anchor_set: [TODO:description]
        :return: [TODO:description]
        """
        key = ''.join(sorted(list(anchor_set)))
        try:
            pattern, _ = self._polytree[key][-1]
            return pattern
        except KeyError:
            return None

    def update_associated_pattern(self, pattern: GraphPattern,
                                  fact_set: set[Statement]) -> None:
        pass


class ValidationReport():

    class Grade(Enum):
        FAILED = auto()
        SUSPICIOUS = auto()
        PASSED = auto()

    def __init__(self, pattern: GraphPattern, fact_set: set[Statement],
                 timestamp: datetime, grade: Grade, metadata: dict[str, str])\
            -> None:
        self.pattern = pattern
        self.fact_set = fact_set
        self.timestamp = timestamp
        self.grade = grade
        self.metadata = metadata

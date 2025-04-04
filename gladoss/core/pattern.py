#! /usr/bin/env python

from __future__ import annotations
from collections import Counter
from copy import deepcopy
from datetime import datetime
from enum import Enum, auto
from threading import Lock
import logging
import sys
from typing import Any, Dict, Iterator, List, Optional, Collection, Set, Union

import numpy as np
import scipy as sp
from gladoss.core.multimodal.datatypes import XSD_CONTINUOUS, XSD_DISCRETE, cast_literal, infer_datatype
from rdf.graph import Statement
from rdf.namespaces import XSD
from rdf.terms import IRIRef, Literal, Resource

from gladoss.core.utils import gen_id, match_facts_to_patterns


logger = logging.getLogger(__name__)


class Distribution():
    def __init__(self, rng: np.random.Generator,
                 sample_size: int = -1,
                 decay: int = -1,
                 dtype: Optional[IRIRef] = None) -> None:
        """ Distribution over samples.

        :param decay: time until seen sample is forgotten
        """
        self._rng = rng
        self.samples = Counter()
        self.sample_size = sample_size
        self.decay = decay
        self._t = 0
        self._decay_tracker = dict()
        self.dtype = dtype

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
            self._decay_tracker[t_decay] = [sample]

        # increment time
        self._forward()

    @staticmethod
    def create_from(rng: np.random.Generator,
                    resource: Resource, sample_size: int,
                    decay: int) -> Distribution:
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
                                              sample_size=sample_size,
                                              decay=decay,
                                              dtype=dtype)
            elif dtype in XSD_DISCRETE:
                dist = DiscreteDistribution(rng=rng,
                                            sample_size=sample_size,
                                            decay=decay,
                                            dtype=dtype)
            else:
                raise NotImplementedError()

            # cast value to appropriate format and add to distribution
            value = cast_literal(dtype, resource.value)
            dist.addSample(value)

        else:  # IRIRef
            # create a new distribution and add values
            dist = DiscreteDistribution(rng=rng,
                                        sample_size=sample_size,
                                        decay=decay)

            dist.addSample(resource)

        return dist

    def _forward(self):
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
                 sample_size: int = 10, resolution: int = -1,
                 dtype: Optional[IRIRef] = None) -> None:
        """ Continuous distribution over Real numbers.

        :param decay: time until seen sample is forgotten
        :param resolution: number of significant figures
        """
        super().__init__(rng, sample_size, decay, dtype)
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
        # TODO: include sample_size over time
        # TODO: check if correct
        p_ltail = 2 * sp.stats.norm.cdf(sample, loc=self.loc, scale=self.scale)
        if sample < self.loc:
            return p_ltail
        elif sample > self.loc:
            return 1 - p_ltail  # right tail
        else:
            return 1.0

        # TODO: alternative idea: check if value falls outside .95 or .99
        # confidence interval
        # sp.stats.norm(mu, sigma).interval(CI_level)

    def loglikelihood(self) -> float:
        """ Return log likelihood L of the distribution parameters theta
            given the observed samples S: L(theta|S)

        :return: a goodness of fit measurement
        """
        return float(
            np.sum(np.log(sp.stats.norm.pdf(list(self.samples.elements()),
                                            self.loc,
                                            self.scale))))

    def __deepcopy__(self, memo) -> ContinuousDistribution:
        """ Create a copy which deepcopies only the dynamic
            elements whereas it creates references to static
            and immutable objects.

        :param memo [TODO:type]: [TODO:description]
        :return: [TODO:description]
        """
        dist = ContinuousDistribution(rng=self._rng, decay=self.decay,
                                      sample_size=self.sample_size,
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
    def __init__(self, rng: np.random.Generator, sample_size: int = 10,
                 decay: int = -1, dtype: Optional[IRIRef] = None) -> None:
        """ Discrete distribution

        :param decay: time until seen sample is forgotten
        """
        super().__init__(rng, sample_size, decay, dtype)

        self.n = 0
        self.k = list()
        self.p = 0.

    def fit(self) -> None:
        # FIXME: check if needed (why not on the fly?)
        # fit multinomial distribution (k >=2, n >= 1)

        # FIXME: Placeholder
        self.n = self.sample_size
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
        if self.samples.total() < self.sample_size:
            logger.info("sample_size size exceeds number of observed samples")

        # time window and corresponding samples TODO: disconnect from decay
        t_max = max(self._decay_tracker.keys())
        window = [t for t in self._decay_tracker.keys()
                  if t in range(t_max-self.sample_size, t_max+1)]
        samples = [v for t in window for v in self._decay_tracker[t]]

        # add observed sample
        samples.append(sample)

        # compute counts and probabilities
        samples_counter = Counter(samples)
        x, p = list(), list()
        sample_i = -1
        for i, (value, freq) in enumerate(samples_counter.items()):
            x.append(freq)
            p.append(freq/self.sample_size)

            if value == sample:
                # keep track of sample index
                sample_i = i

        # compute probability
        prob = sp.stats.multinomial.pmf(x=x, n=len(samples), p=p)
        # FIXME: idea: compare to ideal prob?

        # compute confidence interval - recast problem as computing
        # binomial CI of observing vs not observing sample
        binom_result = sp.stats.binomtest(k=x[sample_i],
                                          n=self.sample_size,
                                          p=p[sample_i])
        ci_lb, ci_hb = binom_result.proportion_ci()

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
                                    sample_size=self.sample_size,
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


class AssertionPattern():
    def __init__(self,  head: IRIRef | DiscreteDistribution,
                 relation: IRIRef,
                 tail: IRIRef | Literal | Distribution,
                 identifier: str) -> None:
        """ Initialize a new AssertionPattern.

        :param head: the subject of an assertion or its distribution
        :param relation: the predicate of an assertion
        :param tail: the object of an assertion or its distribution
        """
        self.head = head
        self.relation = relation
        self.tail = tail
        self._id = identifier

    def weak_match(self, fact: Statement) -> bool:
        """ Check if the provided fact is a likely match to
            this assertion pattern, by comparing whether
            elements are the same or appropriate fits. This
            will fail when the same subject - relation -
            object type is present in more than one statement.

        :param other: [TODO:description]
        :return: [TODO:description]
        """
        return isinstance(fact, Statement)\
            and (type(self.relation) is type(fact.predicate)
                 and self.relation == fact.predicate)\
            and (isinstance(self.head, DiscreteDistribution)
                 or (isinstance(self.head, IRIRef)
                     and self.head == fact.subject))\
            and ((isinstance(self.tail, Resource)
                  and infer_datatype(self.tail) == infer_datatype(fact.object))
                 or (isinstance(self.tail, DiscreteDistribution)
                     and isinstance(fact.object, IRIRef))
                 or (isinstance(self.tail, ContinuousDistribution)
                     and isinstance(fact.object, Literal)
                     and infer_datatype(fact.object) == self.tail.dtype))

    def strong_match(self, fact: Statement) -> bool:
        """ Check if the provided fact is a likely match to
            this assertion pattern, by comparing whether
            elements are the same or if the values fall
            within the distributions, if present.

        :param other: [TODO:description]
        :return: [TODO:description]
        """
        return self.weak_match(fact)  # TODO

    @staticmethod
    def create_from(rng: np.random.Generator,
                    assertion: Statement) -> AssertionPattern:
        """ Create a new assertion pattern from the provided
            assertion. This assumes that all three elements
            are static, until possible future observations
            challenge these beliefs.

        :param rng: [TODO:description]
        :param assertion: [TODO:description]
        :return: [TODO:description]
        """
        head = assertion.subject
        relation = assertion.predicate
        tail = assertion.object

        return AssertionPattern(head, relation, tail,
                                identifier=gen_id(rng))

    def update_from(self, rng: np.random.Generator,
                    assertion: Statement, sample_size: int,
                    decay: int) -> AssertionPattern:
        """ Update the assertion pattern with a new observation
            by copying the pattern and updating its head and/or
            tail elements. Creates a new distribution on the head
            and/or tail if we observe a value that is different
            from the one the original pattern was instantiated
            with; this updates our assumption from the element
            being a static value to it being a dynamic one.

        :param rng: [TODO:description]
        :param assertion: [TODO:description]
        :param sample_size: [TODO:description]
        :param decay: [TODO:description]
        :return: The updated assertion pattern (a copy)
        """
        ap_upd = deepcopy(self)

        def update_element(reference: IRIRef | Literal | Distribution,
                           resource: Resource) -> Distribution:
            """ Add the given value to an appropriate distribution.
                Creates a new distribution if none exists yet (in
                which case we observe two different values).

            :param reference: [TODO:description]
            :param resource: [TODO:description]
            :return: [TODO:description]
            """
            dist = reference
            if not isinstance(reference, Distribution):
                # create a new distribution and add values
                dist = Distribution.create_from(rng=rng, resource=reference,
                                                sample_size=sample_size,
                                                decay=decay)
            if isinstance(resource, Literal):
                dtype = infer_datatype(resource)
                assert dist.dtype == dtype

                # cast value to appropriate format and add to distribution
                value = cast_literal(dtype, resource.value)
                dist.addSample(value)
            else:  # IRIRef
                dist.addSample(resource)

            return dist  # set distribution

        # check if the assertion matches
        assert ap_upd.relation == assertion.predicate

        # evaluate head if necessary
        if not (type(ap_upd.head) is type(assertion.subject)
                and ap_upd.head == assertion.subject):
            ap_upd.head = update_element(ap_upd.head, assertion.subject)

        # evaluate tail if necessary
        if not (type(ap_upd.tail) is type(assertion.obbject)
                and ap_upd.tail == assertion.object):
            ap_upd.tail = update_element(ap_upd.tail, assertion.object)

        return ap_upd

    def __deepcopy__(self, memo) -> AssertionPattern:
        """ Create a deep copy of this assertion pattern which
            creates references to static elements but which
            deep copies all dynamic elements. Note that the
            copied pattern will have the same identifier as
            the original: 'original == copy' wil be true.

        :param memo [TODO:type]: [TODO:description]
        """
        head = self.head
        if isinstance(self.head, Distribution):
            head = deepcopy(self.head)

        tail = self.tail
        if isinstance(self.tail, Distribution):
            tail = deepcopy(self.tail)

        return AssertionPattern(head=head, relation=self.relation, tail=tail,
                                identifier=self._id)

    def __eq__(self, other: Any) -> bool:
        return self._id == other._id

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
    track of the connections of these assertions.
    """
    def __init__(self, pattern: Collection[AssertionPattern],
                 anchors: Collection[Resource], threshold: int,
                 decay: int) -> None:
        self.pattern = list(pattern)
        self.anchors = sorted(list(anchors))

        # # track which assertion patterns are connected to one or more anchors
        # self._anchor_connections_head = {ap._id for ap in self.pattern
        #                                  if ap.head in self.anchors}
        # self._anchor_connections_tail = {ap._id for ap in self.pattern
        #                                  if ap.tail in self.anchors}

        id_lst = [ap._id for ap in self.pattern]
        self._id_to_assertion_map = {_id: i for i, _id in enumerate(id_lst)}

        # number of time steps (with decay) that an assertion is present
        # before including it into the pattern of nominal behaviour.
        self.threshold = threshold

        # number of time steps that an existin assertion is absent before
        # removing it from the pattern of nominal behaviour
        self.decay = decay

        self._t = 0
        self._decay_tracker = dict()

        # track frequencyof assertions by their identifiers
        self._freq_tracker = Counter(id_lst)

        # track newly observed assertions (added on reaching threshold)
        self._under_consideration = list()
        self._id_to_consideration_map = dict()

        # schedule future decay of assertion patterns
        if self.decay > 0:
            t_decay = (self._t + self.decay) % sys.maxsize
            self._decay_tracker[t_decay] = id_lst

    def update(self, aPatterns: Collection[AssertionPattern]) -> None:
        for ap in aPatterns:
            if ap._id not in self._freq_tracker.keys():
                # add unknown assertion for consideration
                self._under_consideration.append(ap)
                self._id_to_consideration_map[ap._id]\
                    = len(self._under_consideration) - 1
                self._freq_tracker[ap._id] = 0
            elif ap._id in self._id_to_assertion_map.keys():
                i = self._id_to_assertion_map[ap._id]
                self.pattern[i] = ap  # replace with updated ap
            else:  # under consideration
                i = self._id_to_assertion_map[ap._id]
                self._under_consideration[i] = ap

            self._freq_tracker[ap._id] += 1  # increase count

        # schedule future decay of assertion patterns
        if self.decay > 0:
            t_decay = (self._t + self.decay) % sys.maxsize
            self._decay_tracker[t_decay] = {ap._id for ap in aPatterns}

        # increment time
        self._forward()

    def __len__(self) -> int:
        """ Return the number of assertions

        :rtype: int
        """
        return len(self.pattern)

    def _forward(self):
        """ Update the distribution by a single time step.
        """
        self._t += 1
        if self._t == sys.maxsize:
            self._t = 0

        if self.threshold > 0:
            self._consider()

        if self.decay > 0:
            self._decay()

    def _consider(self) -> None:
        """ Add new assertions which have reached the threshold.
        """
        remove_set = set()
        for ap in self._under_consideration:
            if ap._id in self._freq_tracker.keys():
                if self._freq_tracker[ap._id] >= self.threshold:
                    # add assertion to pattern
                    self.pattern.append(ap)
                    self._id_to_assertion_map[ap._id] = len(self.pattern) - 1

                    remove_set.add(ap._id)
                    del self._id_to_consideration_map[ap._id]

                    continue

                if self._freq_tracker[ap._id] <= 0:
                    # assertion decayed
                    remove_set.add(ap._id)
                    del self._id_to_consideration_map[ap._id]

        self._under_consideration = [ap for ap in self._under_consideration
                                     if ap._id not in remove_set]

    def _decay(self) -> None:
        """ Forget about assertions that have exceeded their decay period.
        """
        if self._t in self._decay_tracker.keys():
            for ap in self._decay_tracker[self._t]:
                if ap._id in self._freq_tracker.keys():
                    # decrease sample count
                    self._freq_tracker[ap._id] -= 1

                if self._freq_tracker[ap._id] <= 0:
                    # remove sample from index
                    del self._freq_tracker[ap._id]

                # clean up decay tracker
                del self._decay_tracker[self._t]

    def __deepcopy__(self, memo) -> GraphPattern:
        gp = GraphPattern(pattern=[ap for ap in self.pattern],
                          anchors=self.anchors,
                          threshold=self.threshold,
                          decay=self.decay)

        gp._t = self._t
        gp._decay_tracker = {k: v for k, v in self._decay_tracker.items()}
        gp._freq_tracker = Counter(self._freq_tracker.elements())
        gp._under_consideration = [gp for gp in self._under_consideration]
        gp._id_to_consideration_map\
            = {gp._id: i for i, gp in  enumerate(gp._under_consideration)}

        return gp

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
                                self.pattern]) + "}"

    def __hash__(self) -> int:
        return hash(str(self))


class PatternVault():
    def __init__(self) -> None:
        self._polytree = dict()
        self._lock = Lock()

    def add_pattern(self, pattern: GraphPattern) -> None:
        """ Add new graph pattern to pattern vault, by creating a
            new tree with the given pattern as root. This operation
            uses the anchors as key for the associated tree,
            includes a timestamp to record the moment of creation,
            and is thread safe.

        :param pattern: [TODO:description]
        """
        key = ''.join(pattern.anchors)

        self._lock.acquire()
        try:
            assert key not in self._polytree.keys()
            self._polytree[key] = [(pattern, datetime.now())]
        except Exception as e:
            logger.error(f"Unable to add new pattern to pattern vault: {e}")
        finally:
            self._lock.release()

    def update_pattern(self, pattern: GraphPattern) -> None:
        key = ''.join(pattern.anchors)
        try:
            # TODO: compress old version
            # TODO: add timestamp?
            self._polytree[key].append((pattern, datetime.now()))
        except KeyError:
            return

    @staticmethod
    def create_assertion_pattern(rng: np.random.Generator,
                                 assertion: Statement) -> AssertionPattern:
        """ Return a new assertion pattern that belongs to the
            provided assertion. The returned pattern is assigned
            a unique identifier, and assumes (until observations
            claim otherwise) that all elements are static.

        :param rng: [TODO:description]
        :param assertion: [TODO:description]
        :return: [TODO:description]
        """
        return AssertionPattern.create_from(rng=rng, assertion=assertion)

    @staticmethod
    def create_graph_pattern(rng: np.random.Generator,
                             facts: Collection[Statement],
                             anchors: Collection[Resource],
                             threshold: int = -1,
                             decay: int = -1) -> GraphPattern:
        """ Return a new graph pattern from the provided facts
            and anchors by creating assertion patterns for each
            fact and by adding these to an empty graph pattern.

        :param rng: [TODO:description]
        :param facts: [TODO:description]
        :param anchors: [TODO:description]
        :param threshold: [TODO:description]
        :param decay: [TODO:description]
        :return: [TODO:description]
        """
        # create list of assertion patterns from given set of facts
        pattern = [PatternVault.create_assertion_pattern(rng, assertion)
                   for assertion in facts]

        return GraphPattern(pattern=pattern, anchors=anchors,
                            threshold=threshold, decay=decay)

    def find_associated_graph_pattern(self, anchors: Collection[Resource])\
            -> GraphPattern | None:
        """ Find and return the most recent associated graph pattern. This
            operation uses the anchors as keys and is thread safe.

        :param anchors: [TODO:description]
        :return: [TODO:description]
        """
        key = ''.join(sorted(list(anchors)))
        self._lock.acquire()
        try:
            pattern, _ = self._polytree[key][-1]
            return pattern
        except (KeyError, IndexError):
            return None
        finally:
            self._lock.release()

    def update_associated_graph_pattern(self, gPattern: GraphPattern,
                                        facts: Collection[Statement]) -> None:
        # determine matching assertions from the associated graph pattern
        # then copy and update

        # copy graph pattern
        gp = deepcopy(gPattern)

        # find pairs of facts and associated assertion patterns
        fact_ap_pairs, unmatched = match_facts_to_patterns(facts, gp.pattern)

        # copy assertion patterns and update copies with new obsercations
        ap_upd = {ap.update_from(fact) for fact, ap in fact_ap_pairs}

        # find pairs for assertion patterns under consideration 
        uc_upd = set()
        if len(gp._under_consideration) > 0:
            fact_uc_pairs, unmatched = match_facts_to_patterns(
                    unmatched,
                    gp._under_consideration)

            uc_upd = {ap.update_from(fact) for fact, ap in fact_uc_pairs}


        # create new assertion patterns for unmatched observations
        ap_new = {AssertionPattern.create_from(rng, fact)
                  for fact in unmatched}

        # update copy with new assertion patterns
        gp.update(ap_upd | uc_upd | ap_new)


class ValidationReport():

    class Grade(Enum):
        FAILED = auto()
        SUSPICIOUS = auto()
        PASSED = auto()

    def __init__(self, pattern: GraphPattern, facts: Collection[Statement],
                 timestamp: datetime, grade: Grade, metadata: dict[str, str])\
            -> None:
        self.pattern = pattern
        self.facts = facts
        self.timestamp = timestamp
        self.grade = grade
        self.metadata = metadata

#! /usr/bin/env python

from __future__ import annotations
from collections import Counter
from copy import deepcopy
from datetime import datetime
from threading import Lock
import logging
import sys
from types import SimpleNamespace
from typing import Any, Callable, Optional, Collection

from rdf.graph import Statement
from rdf.terms import IRIRef, Literal, Resource

from gladoss.core.multimodal.datatypes import cast_literal, infer_datatype
from gladoss.core.stats import (ContinuousDistribution, DiscreteDistribution,
                                Distribution)
from gladoss.core.utils import match_facts_to_patterns


logger = logging.getLogger(__name__)


def create_assertion_pattern(mkid: Callable,
                             assertion: Statement) -> AssertionPattern:
    """ Return a new assertion pattern that belongs to the
        provided assertion. The returned pattern is assigned
        a unique identifier, and assumes (until observations
        claim otherwise) that all elements are static.

    :param rng: [TODO:description]
    :param assertion: [TODO:description]
    :return: [TODO:description]
    """
    return AssertionPattern.create_from(mkid(), assertion=assertion)


def create_graph_pattern(mkid: Callable,
                         graph: Collection[Statement],
                         graph_id: str,
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
    # FIXME: link connected assertions
    gpattern = [create_assertion_pattern(mkid, assertion)
                for assertion in graph]

    return GraphPattern(pattern=gpattern, identifier=graph_id,
                        threshold=threshold, decay=decay)


def update_graph_pattern(mkid: Callable, gPattern: GraphPattern,
                         facts: Collection[Statement],
                         config: SimpleNamespace) -> GraphPattern:
    """ Return an updated copy of the provided graph pattern by
        first pairing the provided statements with their associated
        assertion patterns, and by then updating these patterns
        with the new observations.

    :param gPattern: [TODO:description]
    :param facts: [TODO:description]
    """
    # find pairs of facts and associated assertion patterns
    # next update copies thereof with new observations
    fact_ap_pairs, unmatched = match_facts_to_patterns(
            facts,
            gPattern.pattern)
    ap_upd = {ap.update_from(fact, config)
              for fact, ap in fact_ap_pairs}

    # find pairs for assertion patterns under consideration
    # only consider the facts that haven't been matched in the previous step
    uc_upd = set()
    if len(gPattern._under_consideration) > 0:
        fact_uc_pairs, unmatched = match_facts_to_patterns(
                unmatched,
                gPattern._under_consideration)

        uc_upd = {ap.update_from(fact, config)
                  for fact, ap in fact_uc_pairs}

    # create new assertion patterns for unmatched observations
    ap_new = {AssertionPattern.create_from(mkid(), fact)
              for fact in unmatched}

    # update graph pattern with updated and new assertion patterns
    gp_upd = gPattern.update_from(ap_upd | uc_upd | ap_new)

    return gp_upd


class AssertionPattern():
    def __init__(self,  head: IRIRef | DiscreteDistribution,
                 relation: IRIRef,
                 tail: IRIRef | Literal | Distribution,
                 identifier: str, _t: Optional[int] = None) -> None:
        """ Initialize a new AssertionPattern.

        :param head: the subject of an assertion or its distribution
        :param relation: the predicate of an assertion
        :param tail: the object of an assertion or its distribution
        """
        self.head = head
        self.relation = relation
        self.tail = tail
        self._id = identifier
        self._t = 1 if _t is None else _t

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
            and (isinstance(self.tail, IRIRef)
                 or (isinstance(self.tail, Literal)
                     and infer_datatype(self.tail)
                     == infer_datatype(fact.object))
                 or (isinstance(self.tail, DiscreteDistribution)
                     and ((isinstance(fact.object, IRIRef)
                           and self.tail.dtype is None)
                          or (isinstance(fact.object, Literal)
                              and infer_datatype(fact.object)
                              == self.tail.dtype)))
                 or (isinstance(self.tail, ContinuousDistribution)
                     and isinstance(fact.object, Literal)
                     and infer_datatype(fact.object) == self.tail.dtype))

    def strong_match(self, fact: Statement) -> bool:
        """ Check if the provided fact is a likely match to
            this assertion pattern, by comparing whether
            elements are the same or if the values fall
            within the distributions, if present. To reduce
            computation complexity, here the distributions
            tests are simplified by checking if the supplied
            values are within the same range.

        :param other: [TODO:description]
        :return: [TODO:description]
        """
        return self.weak_match(fact)\
            and ((isinstance(self.tail, Resource)
                  and self.tail == fact.object)
                 or (isinstance(self.tail, DiscreteDistribution)
                     and fact.object in self.tail.data)
                 or (isinstance(self.tail, ContinuousDistribution)
                     and float(fact.object) in range(min(self.tail.data),
                                                     max(self.tail.data))))

    @staticmethod
    def create_from(identifier: str,
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
                                identifier=identifier)

    def update_from(self, assertion: Statement,
                    config: SimpleNamespace)\
            -> AssertionPattern:
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

        # FIXME: link distributions of connected assertions
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
            num_times = 1
            if not isinstance(reference, Distribution):
                # create a new distribution and add values
                dist = Distribution.create_from(
                        resource=reference,
                        decay=config.pattern_decay,
                        resolution=config.pattern_resolution)

                # add old value this number of times, which equals the
                # contiguous stretch of time that this value was seen
                num_times = self._t
            if isinstance(resource, Literal):
                dtype = infer_datatype(resource)
                assert dist.dtype == dtype

                # cast value to appropriate format and add to distribution
                value = cast_literal(dtype, resource.value)
                for _ in range(num_times):
                    dist.addSample(value)
            else:  # IRIRef
                for _ in range(num_times):
                    dist.addSample(resource)

            return dist  # set distribution

        # check if the assertion matches
        assert ap_upd.relation == assertion.predicate

        # evaluate head if necessary
        if not (type(ap_upd.head) is type(assertion.subject)
                and ap_upd.head == assertion.subject):
            ap_upd.head = update_element(ap_upd.head, assertion.subject)

        # evaluate tail if necessary
        if not (type(ap_upd.tail) is type(assertion.object)
                and ap_upd.tail == assertion.object):
            ap_upd.tail = update_element(ap_upd.tail, assertion.object)

        ap_upd._forward()  # forward time by one step

        return ap_upd

    def _forward(self):
        """ Forward the pattern by a single time step.
        """
        self._t += 1
        if self._t == sys.maxsize:
            self._t = 0

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
                                identifier=self._id, _t=self._t)

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
                 identifier: str, threshold: int,
                 decay: int) -> None:
        self.pattern = sorted(list(pattern))
        self._id = identifier

        # keep track of time
        # time is incremented on update or manually
        self._t = 0

        # identifiers of the assertion patterns that make up the graph pattern
        # track location in list for fast retrieval
        id_lst = [ap._id for ap in self.pattern]
        self._id_to_assertion_map = {_id: i for i, _id in enumerate(id_lst)}

        # number of time steps (with decay) that an assertion is present
        # before including it into the pattern of nominal behaviour.
        self.threshold = threshold

        # number of time steps that an existin assertion is absent before
        # removing it from the pattern of nominal behaviour
        self.decay = decay

        # track frequencyof assertions by their identifiers
        self._freq_tracker = Counter(id_lst)

        # track newly observed assertions (added on reaching threshold)
        self._under_consideration = list()
        self._id_to_consideration_map = dict()

        # schedule future decay of assertion patterns
        self._decay_tracker = dict()
        if self.decay > 0:
            t_decay = (self._t + self.decay) % sys.maxsize
            self._decay_tracker[t_decay] = id_lst

    def update_from(self, aPatterns: Collection[AssertionPattern])\
            -> GraphPattern:
        """ Return a copy of this graph pattern that has been updated
            with the provided assertion patterns (new observations).
            The updated graph pattern will be fowarded in time by
            a single epoch.

        :param aPatterns: [TODO:description]
        :return: [TODO:description]
        """
        gp = deepcopy(self)

        # update assertion patterns
        for ap in aPatterns:
            if ap._id in gp._id_to_assertion_map.keys():
                # known member of the current graph pattern
                i = gp._id_to_assertion_map[ap._id]
                gp.pattern[i] = ap  # replace with updated ap
            elif ap._id in gp._id_to_consideration_map.keys():
                # known assertion pattern under consideration
                i = gp._id_to_consideration_map[ap._id]
                gp._under_consideration[i] = ap
            else:  # unknown assertion pattern: add for consideration
                gp._under_consideration.append(ap)
                gp._id_to_consideration_map[ap._id]\
                    = len(gp._under_consideration) - 1
                gp._freq_tracker[ap._id] = 0

            gp._freq_tracker[ap._id] += 1  # increase count

        # schedule future decay of assertion patterns
        if gp.decay > 0:
            t_decay = (gp._t + gp.decay) % sys.maxsize
            gp._decay_tracker[t_decay] = {ap._id for ap in aPatterns}

        # increment time
        gp._forward()

        return gp

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
                          identifier=self._id,
                          threshold=self.threshold,
                          decay=self.decay)

        gp._t = self._t
        gp._decay_tracker = {k: v for k, v in self._decay_tracker.items()}
        gp._freq_tracker = Counter(self._freq_tracker.elements())
        gp._under_consideration = [gp for gp in self._under_consideration]
        gp._id_to_consideration_map\
            = {gp._id: i for i, gp in enumerate(gp._under_consideration)}

        return gp

    def __eq__(self, other) -> bool:
        """ Returns true if it is the same graph pattern at the same
            moment in time.

        :param other [TODO:type]: [TODO:description]
        :return: [TODO:description]
        """
        return type(self) is type(other)\
            and self._id == other._id\
            and self._t == other._t

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

    def add_graph_pattern(self, pattern: GraphPattern) -> None:
        """ Add new graph pattern to pattern vault, by creating a
            new tree with the given pattern as root. This operation
            includes a timestamp to record the moment of creation,
            and is thread safe.

        :param pattern: [TODO:description]
        """
        key = pattern._id

        self._lock.acquire()
        try:
            assert key not in self._polytree.keys()
            self._polytree[key] = [(pattern, datetime.now())]
        except Exception as e:
            logger.error(f"Unable to add new pattern vault entry: {e}")
        finally:
            self._lock.release()

    def rmv_graph_pattern(self, pattern: GraphPattern) -> None:
        """ Remove registered graph pattern from the vault. This
            operation removes the entire tree and is thread safe.

        :param pattern: [TODO:description]
        """
        key = pattern._id

        self._lock.acquire()
        try:
            assert key in self._polytree.keys()
            del self._polytree[key]
        except Exception as e:
            logger.error(f"Unable to remove pattern vault entry: {e}")
        finally:
            self._lock.release()

    def prune_graph_pattern(self, pattern: Optional[GraphPattern]) -> None:
        """ Prune the tree of the provided graph pattern by replacing
            the entire tree with a new tree that only contains the
            most recent graph pattern. Do this for all registered
            graph patterns if none is provided. This operation is
            thread safe

        :param pattern: [TODO:description]
        """
        prune_lst = [pattern]
        if pattern is None:
            prune_lst = [pattern._id for pattern in self._polytree.keys()]

        self._lock.acquire()
        try:
            for key in prune_lst:
                assert key in self._polytree.keys()
                self._polytree[key] = [self._polytree[key][-1]]
        except Exception as e:
            logger.error(f"Unable to prune pattern vault entry: {e}")
        finally:
            self._lock.release()

    def update_graph_pattern(self, pattern: GraphPattern) -> None:
        """ Update registered graph pattern by adding the updated
            pattern as a new leaf to the tree. The previous version
            of the pattern automatically becomes a non-terminal
            vertex in the tree. This operation is thread safe.

        :param pattern: [TODO:description]
        """
        key = pattern._id

        self._lock.acquire()
        try:
            # TODO: compress old version
            self._polytree[key].append((pattern, datetime.now()))
        except Exception as e:
            logger.error(f"Unable to update pattern vault entry: {e}")
        finally:
            self._lock.release()

    def find_associated_graph_pattern(self, key: str)\
            -> GraphPattern | None:
        """ Find and return the most recent associated graph pattern. This
            operation is thread safe.

        :return: [TODO:description]
        """
        self._lock.acquire()
        try:
            pattern, _ = self._polytree[key][-1]
            return pattern
        except (KeyError, IndexError):
            return None
        finally:
            self._lock.release()

    def __len__(self) -> int:
        return len(self._polytree.keys())

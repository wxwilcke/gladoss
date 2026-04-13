#! /usr/bin/env python

from __future__ import annotations
import bz2
from collections import Counter
from copy import deepcopy
import pickle
from datetime import datetime
from threading import RLock
import logging
import sys
from types import SimpleNamespace
from typing import Any, Callable, Optional, Collection

from rdf.graph import Statement
from rdf.namespaces import XSD
from rdf.terms import IRIRef, Literal, Resource

from gladoss.core.multimodal.datatypes import cast_literal, infer_datatype
from gladoss.core.stats import (ContinuousDistribution, DiscreteDistribution,
                                Distribution)
from gladoss.core.utils import infer_class


logger = logging.getLogger(__name__)


def create_assertion_pattern(mkid: Callable,
                             assertion: Statement,
                             anchor: IRIRef)\
        -> AssertionPattern:
    """ Return a new assertion pattern that belongs to the
        provided assertion. The returned pattern is assigned
        a unique identifier, and assumes (until observations
        claim otherwise) that all elements are static.

    :param rng: [TODO:description]
    :param assertion: [TODO:description]
    :return: [TODO:description]
    """
    return AssertionPattern.create_from(mkid(), assertion=assertion,
                                        anchor=anchor)


def create_graph_pattern(mkid: Callable,
                         graph: Collection[Statement],
                         graph_id: str,
                         threshold: int = -1,
                         decay: int = -1) -> GraphPattern:
    """ Return a new graph pattern from the provided facts
        by creating assertion patterns (AP) for each fact.
        Each AP holds a subject-centric patterns that belongs
        to the relation-object pair, with the subject type
        as anchor. The generated APs are added to an empty
        graph pattern.

   :param rng: [TODO:description]
    :param facts: [TODO:description]
    :param anchors: [TODO:description]
    :param threshold: [TODO:description]
    :param decay: [TODO:description]
    :return: [TODO:description]
    """
    logger.info(f"Creating new graph pattern ({graph_id})")
    # create assertion patterns from given set of assertions
    structure = dict()
    for i, assertion in enumerate(graph, 1):
        # TODO; accomodate dangling heads
        logger.info(f"Creating new assertion pattern {i}/{len(graph)} "
                    f"({graph_id})")

        anchor = infer_class(assertion.subject, graph)
        ap = create_assertion_pattern(mkid, assertion, anchor)

        structure[ap._id] = ap

    return GraphPattern(structure=structure, identifier=graph_id,
                        threshold=threshold, decay=decay)


def update_graph_pattern(mkid: Callable, gPattern: GraphPattern,
                         graph: Collection[Statement],
                         pattern_map: tuple[list[tuple[Statement,
                                                       AssertionPattern]],
                                            list[tuple[Statement,
                                                       AssertionPattern]],
                                            set[Statement]],

                         config: SimpleNamespace) -> GraphPattern:
    """ Return an updated copy of the provided graph pattern by
        copying and updating the nominal and candidate subpatterns
        with the new observations, and by adding new candidate
        subpatterns.

    :param gPattern: [TODO:description]
    :param facts: [TODO:description]
    """
    logger.info(f"Updating graph data ({gPattern._id})")
    # unpack assertion to assertion pattern map
    assertion_ap_pairs, assertion_uc_pairs, unmatched = pattern_map

    # update nonimal assertion pattern with new observations
    ap_upd = {ap.update_from(assertion, config)
              for assertion, ap in assertion_ap_pairs}

    # update candidate assertion pattern with new observations
    uc_upd = {ap.update_from(assertion, config)
              for assertion, ap in assertion_uc_pairs}

    # create new assertion patterns for unmatched observations
    ap_new = set()
    for assertion in unmatched:
        anchor = infer_class(assertion.subject, graph)
        ap_new.add(AssertionPattern.create_from(mkid(), assertion, anchor))

    # update graph pattern with updated and new assertion patterns
    gp_upd = gPattern.update_from(ap_upd | uc_upd | ap_new)

    return gp_upd


class AssertionPattern():
    def __init__(self,  anchor: IRIRef,
                 relation: IRIRef,
                 value: IRIRef | Literal | Distribution,
                 identifier: str, _t: Optional[int] = None) -> None:
        """ Initialize a new AssertionPattern.

        :param anchor: the class of resource at the head position
        :param relation: the predicate of an assertion
        :param tail: the object of an assertion or its distribution
        """
        self.anchor = anchor
        self.relation = relation
        self.value = value
        self._id = identifier
        self._t = 1 if _t is None else _t

    def weak_match(self, assertion: Statement, graph: Collection[Statement])\
            -> bool:
        """ Check if the provided assertion is a likely match to
            this assertion pattern, by comparing whether
            elements are the same or appropriate fits. This
            will fail when the same subject type - relation -
            object type is present in more than one statement.

        :param other: [TODO:description]
        :return: [TODO:description]
        """
        anchor = infer_class(assertion.subject, graph)

        return isinstance(assertion, Statement)\
            and (isinstance(assertion.predicate, IRIRef)
                 and self.relation == assertion.predicate)\
            and (isinstance(anchor, IRIRef)
                 and self.anchor == anchor)\
            and ((isinstance(self.value, IRIRef)
                  and isinstance(assertion.object, IRIRef)
                  and infer_class(self.value, graph)
                  == infer_class(assertion.object, graph))
                 or (isinstance(self.value, Literal)
                     and isinstance(assertion.object, Literal)
                     and infer_datatype(self.value)
                     == infer_datatype(assertion.object))
                 or (isinstance(self.value, DiscreteDistribution)
                     and ((isinstance(assertion.object, IRIRef)
                           and self.value.dtype == XSD+'anyURI')
                          or (isinstance(assertion.object, Literal)
                              and infer_datatype(assertion.object)
                              == self.value.dtype)))
                 or (isinstance(self.value, ContinuousDistribution)
                     and isinstance(assertion.object, Literal)
                     and infer_datatype(assertion.object) == self.value.dtype))

    def strong_match(self, assertion: Statement,
                     graph: Collection[Statement]) -> bool:
        """ Check if the provided assertion is a likely match to
            this assertion pattern, by comparing whether
            elements are the same or if the values fall
            within the distributions, if present. To reduce
            computation complexity, here the distributions
            tests are simplified by checking if the supplied
            values are within the same range.

        :param other: [TODO:description]
        :return: [TODO:description]
        """
        value = assertion.object
        if isinstance(self.value, Distribution):
            if isinstance(assertion.object, IRIRef):
                value = assertion.object.value
            elif isinstance(assertion.object, Literal):
                dtype = infer_datatype(assertion.object)
                value = cast_literal(dtype, assertion.object)

        return self.weak_match(assertion, graph)\
            and ((isinstance(self.value, Resource)
                  and self.value == assertion.object)
                 or (isinstance(self.value, DiscreteDistribution)
                     and value in self.value.data)
                 or (isinstance(self.value, ContinuousDistribution)
                     and value in range(min(self.value.data),
                                        max(self.value.data))))

    @staticmethod
    def create_from(identifier: str,
                    assertion: Statement,
                    anchor: IRIRef) -> AssertionPattern:
        """ Create a new assertion pattern from the provided
            assertion. This assumes that all three elements
            are static, until possible future observations
            challenge these beliefs.

        :param rng: [TODO:description]
        :param assertion: [TODO:description]
        :return: [TODO:description]
        """
        relation = assertion.predicate
        value = assertion.object

        return AssertionPattern(anchor, relation, value,
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
                assert dist.dtype == dtype, "Literal datatype does not match "\
                                            "distribution datatype"

                # cast value to appropriate format and add to distribution
                value = cast_literal(dtype, resource)
                for _ in range(num_times):
                    dist.addSample(value)
            else:  # IRIRef
                for _ in range(num_times):
                    dist.addSample(resource.value)

            return dist  # set distribution

        # evaluate value if necessary
        if not (type(ap_upd.value) is type(assertion.object)
                and ap_upd.value == assertion.object):
            ap_upd.value = update_element(ap_upd.value, assertion.object)

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

        value = self.value
        if isinstance(self.value, Distribution):
            value = deepcopy(self.value)

        ap = AssertionPattern(anchor=self.anchor, relation=self.relation,
                              value=value, identifier=self._id, _t=self._t)

        return ap

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
        return "(" + ', '.join([str(self.anchor),
                                str(self.relation),
                                str(self.value)]) + ")"


class GraphPattern():
    """ GraphPattern class

    Holds all assertions of a graph pattern and keeps
    track of the connections of these assertions.
    """
    def __init__(self, structure: dict[str, AssertionPattern],
                 identifier: str, threshold: int,
                 decay: int) -> None:
        self.structure = structure
        self._id = identifier

        # number of time steps (with decay) that an assertion pattern is
        # present before including it into the pattern of nominal behaviour.
        self.threshold = threshold

        # number of time steps that an existin assertion pattern is
        # absent before removing it from the pattern of nominal behaviour
        self.decay = decay

        # keep track of time
        # time is incremented on update or manually
        self._t = 1

        # track frequency of assertion patterns by their identifiers
        self._freq_tracker = Counter(set(self.structure))

        # track newly observed assertion patterns
        # these are added to the structure upon reaching threshold
        self._under_consideration = dict()

        # schedule future decay of assertion patterns
        self._decay_tracker = dict()
        if self.decay > 0:
            t_decay = (self._t + self.decay) % sys.maxsize
            self._decay_tracker[t_decay] = set(self.structure)

    def update_from(self, updates: Collection[AssertionPattern])\
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
        for ap in updates:
            if ap._id in gp.structure.keys():
                # known member of the current graph pattern
                gp.structure[ap._id] = ap  # just replace
            elif ap._id in gp._under_consideration.keys():
                # known assertion pattern under consideration
                gp._under_consideration[ap._id] = ap  # just replace
            else:  # unknown assertion pattern: add for consideration
                gp._under_consideration[ap._id] = ap
                gp._freq_tracker[ap._id] = 0

            gp._freq_tracker[ap._id] += 1  # increase count

        # schedule future decay of updated assertion patterns
        if gp.decay > 0:
            t_decay = (gp._t + gp.decay) % sys.maxsize
            gp._decay_tracker[t_decay] = {ap._id for ap in updates}

        # increment time
        gp._forward()

        return gp

    def __len__(self) -> int:
        """ Return the number of assertions

        :rtype: int
        """
        return len(self.structure.keys())

    def _forward(self):
        """ Update the distribution by a single time step.
        """
        self._t += 1
        if self._t >= sys.maxsize:
            self._t = 0

        if self.threshold > 0:
            self._consider()

        if self.decay > 0:
            self._decay()

    def _consider(self) -> None:
        """ Add new assertion patterns which have reached the threshold
            to the structure, thereby including them in the nominal behaviour.
        """
        remove_set = set()
        for ap_id in self._under_consideration.keys():
            if ap_id in self._freq_tracker.keys():
                if self._freq_tracker[ap_id] >= self.threshold:
                    # add assertion pattern to graph pattern structure
                    self.structure[ap_id] = self._under_consideration[ap_id]
                    remove_set.add(ap_id)

                    continue

        for ap_id in remove_set:
            del self._under_consideration[ap_id]

    def _decay(self) -> None:
        """ Forget about assertion patterns that have exceeded their
            decay period, by removing them from the structure.
        """
        if self._t in self._decay_tracker.keys():
            for ap_id in self._decay_tracker[self._t]:
                if ap_id in self._freq_tracker.keys():
                    # decrease sample count
                    self._freq_tracker[ap_id] -= 1

                    if self._freq_tracker[ap_id] <= 0:
                        if ap_id in self.structure.keys():
                            del self.structure[ap_id]
                        elif ap_id in self._under_consideration.keys():
                            del self._under_consideration[ap_id]

                        # remove sample from index
                        del self._freq_tracker[ap_id]

            # clean up decay tracker
            del self._decay_tracker[self._t]

    def __deepcopy__(self, memo) -> GraphPattern:
        structure = {k: v for k, v in self.structure.items()}
        gp = GraphPattern(structure=structure,
                          identifier=self._id,
                          threshold=self.threshold,
                          decay=self.decay)

        gp._t = self._t
        gp._decay_tracker = {k: v for k, v in self._decay_tracker.items()}
        gp._freq_tracker = Counter(self._freq_tracker.elements())
        gp._under_consideration\
            = {k: v for k, v in self._under_consideration.items()}

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
        return "{" + "; ".join([str(self.structure[k]) for k in
                                sorted(self.structure.keys())]) + "}"

    def __hash__(self) -> int:
        return hash(str(self))


class PatternVault():
    def __init__(self, lock: RLock, compress: bool = True) -> None:
        self.compress = compress

        self._polytree = dict()
        self._lock = lock

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
        assert len(self._polytree[key]) > 0

        self._lock.acquire()
        try:
            # compress old version
            if self.compress:
                prev, t_prev = self._polytree[key][-1]
                prev = bz2.compress(pickle.dumps(prev))
                self._polytree[-1] = (prev, t_prev)

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

    def __getstate__(self):
        return {k: v for k, v in self.__dict__.items() if k != '_lock'}

    def __setstate__(self, state):
        self.__dict__.update(state)

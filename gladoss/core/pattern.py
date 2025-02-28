#! /usr/bin/env python

from collections import Counter
from __future__ import annotations
from queue import Queue
import sys
from typing import Any, Dict, Iterator, List, Optional, Set, Union
from uuid import uuid4

from gladoss.core.utils import find_root, find_typed_instances
from rdf.graph import Statement
from rdf.terms import IRIRef, Literal


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
                or ((self.parent is None or self.parent == other.parent)\
                    and self.value < other.value)


class Distribution():
    def __init__(self, decay: int = -1) -> None:
        """ Distribution over samples.

        :param decay: time until seen sample is forgotten
        """
        self.samples = Counter()
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

    def __hash__(self) -> int:
        return hash(repr(self))

    def __eq__(self, other: Any) -> bool:
        return type(self) is type(other) and repr(self) == repr(other)

    def __lt__(self, other: Distribution) -> bool:
        return repr(self) < repr(other)


class ContinuousDistribution(Distribution):
    _N = f"\N{MATHEMATICAL BOLD SCRIPT CAPITAL N}"

    def __init__(self, decay: int = -1, resolution: int = -1)\
            -> None:
        """ Continuous distribution over Real numbers.

        :param decay: time until seen sample is forgotten
        :param resolution: number of significant figures
        """
        super().__init__(decay)
        self.resolution = resolution

        self.mu = 0.
        self.sigma = 0.

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
        # fit Gaussian
        # branch off when multiple modes are found (using GMM)

        # FIXME: placeholder
        self.mu = self.samples.most_common(1)[0][1]
        self.sigma = self.mu/6

    def p_value(self, sample: float) -> float:
        # FIXME: placeholder
        return 0.23

    def __str__(self) -> str:
        return f"{ContinuousDistribution._N}({self.mu}, "\
               + f"{self.sigma}\N{SUPERSCRIPT TWO})"

    def __repr__(self) -> str:
        return self.__str__() + f" [decay: {self.decay}, "\
                                + f"res: {self.resolution}, "\
                                + f"n: {self.samples.total()}]"


class DiscreteDistribution(Distribution):
    def __init__(self, decay: int = -1) -> None:
        """ Discrete distribution

        :param decay: time until seen sample is forgotten
        """
        super().__init__(decay)

    def fit(self) -> None:
        # fit multinomial distribution (k >=2, n >= 1)

        # FIXME: Placeholder
        self.n = self.samples.total()
        self.k = self.samples
        self.p = [k/self.n for k in self.k]

    def p_value(self, sample: float) -> float:
        # FIXME: placeholder
        return 0.32


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
                 root: IRIRef,
                 root_type: Optional[IRIRef] = None) -> None:
        self.pattern = assertionPatterns
        self._root = root
        self._root_type = root_type

        # TODO: deal with changes in distribution
        #       assign index to assertions?
        self.assertions = Counter(self.pattern)

    def __len__(self) -> int:
        """ Return the number of assertions

        :rtype: int
        """
        return len(self.pattern)


    def update(self, set[Statements]) -> None:
        pass

    def depth(self) -> int:
        """ Return the length of the longest non-cyclic path via BFS.

        :rtype: int
        """
        d = {self._root: 0}  # record distances to nodes from root
        q = Queue()

        q.put(self._root)
        while not q.empty():
            u = q.get()
            for a in self.pattern:
                if u == a.head and isinstance(a.tail, IRIRef)\
                   and a.tail not in d.keys():
                    d[a.tail] = d[u] + 1  # depth of parent plus one

                    q.put(a.tail)

        return max(d.values())

    def match(self, assertions: set[Statement] = set()) -> bool:
        """ Check if set of statements (ie, a graph) matches
            this pattern. Currently only checks on root node
            and root node type.

        :param assertions: a connected set of statements (a graph)
        :return: True if a match is found else False
        """
        # check if root IRI is the same (and optionally if they share the type)
        return self._root == find_root(assertions, type=self._root_type)

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


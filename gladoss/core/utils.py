#!/usr/bin/env python

from queue import Queue
import sys
from typing import Optional

import numpy as np
import scipy as sp
from rdf import Statement, IRIRef, RDF


def find_root(g: set[Statement], type: Optional[IRIRef] = None)\
        -> IRIRef | None:
    """ Heuristic method to find the root node in a tree. This
        method assumes a subject-oriented graph in which the
        root node has an in-degree of zero. Provide the class
        of the root node for more certainty. Nothing is returned
        if there are multiple candidates.

    :param g: a set of assertions that form a graph
    :param type: the class of the root node
    :return: the assumed root node or None
    """
    root = None

    dangling_nodes = set(a.subject for a in g) - set(a.object for a in g)
    if len(dangling_nodes) >= 1:
        if type is not None:
            rdf_type = RDF + "type"
            for a in g:
                if a.predicate == rdf_type and a.object == type\
                   and a.head in dangling_nodes:
                    root = a.head

                    break

        elif len(dangling_nodes) == 1:
            root = dangling_nodes.pop()

    return root


def find_typed_instances(g: set[Statement]) -> set[IRIRef]:
    """ Return the nodes who have an associated type, together with their type.

    :param g: a set of assertions that form a graph
    :return: a set of nodes and their classes
    """
    out = set()
    rdf_type = RDF + "type"
    for a in g:
        if a.predicate == rdf_type:
            out.add(a.object)

    return out

def trim_graph(g: set[Statement]) -> set[Statement]:
    # remove all ontological info (which is static and doesn't add more info)
    # except for type relations
    pass

def create_graph_proxy(g: set[Statement]):
    proxy = list()
    for a in g:
        pass


def init_rng(seed: Optional[int | float] = None) -> np.random.Generator:
    """ Initiate random state by specified seed. Use in
        scipy instances S with S.random_state = rng.

    :param seed: a positive value
    :return: a RNG instance
    """
    if seed is None:
        seed = np.random.randint(-sys.maxsize//2, sys.maxsize//2)

    return np.random.Generator(np.random.PCG64(np.array([seed])))

#!/usr/bin/env python

import operator
from queue import Queue
import sys
from typing import Optional

import numpy as np
import scipy as sp
from rdf import Statement, IRIRef, RDF, Literal


def find_shortest_paths(g: set[Statement], source: IRIRef, target: IRIRef | Literal)\
        -> list[list[Statement]]:
    """ Find shortest path(s) between two vertices.

    :param g: [TODO:description]
    :param source: [TODO:description]
    :param target: [TODO:description]
    :return: [TODO:description]
    """
    paths = [[a] for a in g if a.subject == source]
    q = Queue()

    q.put(source)
    while not q.empty():
        u = q.get()
        for a in g:
            if u != a.subject:
                # not connected
                continue

            # TODO: exclude cyclic paths
            for path in paths:
                if path[-1].object == u:
                    path_new = [a for a in path]
                    path_new.append(a)

                    paths.append(path_new)

            if a.object != target and isinstance(a.object, IRIRef):
                q.put(a.object)

    # compute lengths and sort in increasing order
    path_len = sorted([(len(path), i) for i, path in enumerate(paths)
                       if path[-1].object == target],
                      key=operator.itemgetter(0)) 

    # keep shortest path(s)
    shortest_paths = [paths[i] for pl, i in path_len
                      if pl <= path_len[0][0]]

    return shortest_paths
                

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
        seed = np.random.randint(sys.maxsize)

    return np.random.Generator(np.random.PCG64(np.array([seed])))

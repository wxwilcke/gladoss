#!/usr/bin/env python

from collections import Counter
import operator
import importlib
import logging
from queue import Queue
import sys
from types import ModuleType
from typing import Collection, Optional, Sequence

import numpy as np
import scipy as sp
from gladoss.adaptors.adaptor import Adaptor
from gladoss.core.pattern import AssertionPattern, GraphPattern
from rdf import Statement, IRIRef, RDF, Literal

logger = logging.getLogger(__name__)

#def find_shortest_paths(g: set[Statement], source: IRIRef, target: IRIRef | Literal)\
#        -> list[list[Statement]]:
#    """ Find shortest path(s) between two vertices.
#
#    :param g: [TODO:description]
#    :param source: [TODO:description]
#    :param target: [TODO:description]
#    :return: [TODO:description]
#    """
#    paths = [[a] for a in g if a.subject == source]
#    q = Queue()
#
#    q.put(source)
#    while not q.empty():
#        u = q.get()
#        for a in g:
#            if u != a.subject:
#                # not connected
#                continue
#
#            # TODO: exclude cyclic paths
#            for path in paths:
#                if path[-1].object == u:
#                    path_new = [a for a in path]
#                    path_new.append(a)
#
#                    paths.append(path_new)
#
#            if a.object != target and isinstance(a.object, IRIRef):
#                q.put(a.object)
#
#    # compute lengths and sort in increasing order
#    path_len = sorted([(len(path), i) for i, path in enumerate(paths)
#                       if path[-1].object == target],
#                      key=operator.itemgetter(0))
#
#    # keep shortest path(s)
#    shortest_paths = [paths[i] for pl, i in path_len
#                      if pl <= path_len[0][0]]
#
#    return shortest_paths
#
#
# def find_root(g: set[Statement], type: Optional[IRIRef] = None)\
#         -> IRIRef | None:
#     """ Heuristic method to find the root node in a tree. This
#         method assumes a subject-oriented graph in which the
#         root node has an in-degree of zero. Provide the class
#         of the root node for more certainty. Nothing is returned
#         if there are multiple candidates.
# 
#     :param g: a set of assertions that form a graph
#     :param type: the class of the root node
#     :return: the assumed root node or None
#     """
#     root = None
# 
#     dangling_nodes = set(a.subject for a in g) - set(a.object for a in g)
#     if len(dangling_nodes) >= 1:
#         if type is not None:
#             rdf_type = RDF + "type"
#             for a in g:
#                 if a.predicate == rdf_type and a.object == type\
#                    and a.head in dangling_nodes:
#                     root = a.head
# 
#                     break
# 
#         elif len(dangling_nodes) == 1:
#             root = dangling_nodes.pop()
# 
#     return root
# 
# 
# def find_typed_instances(g: set[Statement]) -> set[IRIRef]:
#     """ Return the nodes who have an associated type, together with their type.
# 
#     :param g: a set of assertions that form a graph
#     :return: a set of nodes and their classes
#     """
#     out = set()
#     rdf_type = RDF + "type"
#     for a in g:
#         if a.predicate == rdf_type:
#             out.add(a.object)
# 
#     return out


def match_facts_to_patterns(
        facts: Collection[Statement],
        aPatterns: Collection[AssertionPattern])\
                -> tuple[list[tuple], set]:
    """ Find pairs of facts with associated assertion patterns
        by first trying to match on relations, iff unique, and
        else by weakly comparing and finally strongly comparing
        the relation-object pairs.

    :param facts: [TODO:description]
    :param assertionPatterns: [TODO:description]
    :return: [TODO:description]
    """
    # count relations of both inut sets combined
    relation_count = Counter([fact.predicate for fact in facts])
    relation_count.update([ap.relation for ap in aPatterns])

    # match facts to their pattern
    remainder = set()
    fact_ap_pairs = list()
    fact_ap_conflicts = list()
    for fact in facts:
        matches = list()
        for ap in aPatterns:
            if relation_count[fact.predicate] > 2:  # ambiguous match
                if ap.weak_match(fact):
                    # match on relation-object type
                    matches.append(ap)
                elif ap.strong_match(fact):
                    # match on relation-object value
                    matches.append(ap)
            else:  # one-on-one match with relations
                if ap.relation == fact.predicate:
                    matches.append(ap)

        if len(matches) > 1:
            fact_ap_conflicts.append((fact, matches))
        elif len(matches) == 1:  # exact one match found
            fact_ap_pairs.append((fact, matches[-1]))
        else:  # no matches
            remainder.add(fact)

    # conflict resolution: multiple matches per fact
    if len(fact_ap_conflicts) > 0:
        # TODO: implement some conflict resolution
        for fact, _ in fact_ap_conflicts:
            logger.debug(f"Matches multiple assertion patterns: {fact}")

    return fact_ap_pairs, remainder


def init_rng(seed: Optional[int] = None) -> np.random.Generator:
    """ Initiate random state by specified seed. Use in
        scipy instances S with S.random_state = rng.

    :param seed: a positive value
    :return: a RNG instance
    """
    if seed is None:
        seed = np.random.randint(sys.maxsize)

    return np.random.Generator(np.random.PCG64(np.array([seed])))


def import_class(module_map: dict[str, list[str]], name: str) -> Adaptor:
    """ Import specified module and class

    :param module_map: a map with adaptor names mapped to their module path
                       and class name
    :param name: the name of the adaptor
    :return: the specified adaptor class
    """
    cls = None
    try:
        mod_str, class_str = module_map[name]
        module = importlib.import_module(f'gladoss.adaptors.{mod_str}')
        cls = getattr(module, class_str)
    except (AttributeError, KeyError, ImportError) as e:
        print(f"Failed to load adaptor {name}: {e}")
        sys.exit(1)

    return cls


def gen_id(rng: np.random.Generator) -> str:
    """ Generate a random alphanumeric identifier.

    :param rng: [TODO:description]
    :return: [TODO:description]
    """
    a, z = 97, 122
    i_l, i_h = 48, 57

    # generate vocabulary
    ascii_lst = [chr(i) for i in range(a, z+1)]\
        + [chr(i) for i in range(i_l, i_h+1)]

    # sample vocabulary
    id_lst = rng.choice(ascii_lst, size=20)

    return 'U' + ''.join(id_lst)

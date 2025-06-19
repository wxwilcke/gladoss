#!/usr/bin/env python

from collections import Counter
import importlib
import logging
import sys
from typing import Collection, Optional

import numpy as np
from gladoss.adaptors.adaptor import Adaptor

from rdf import IRIRef, Statement
from rdf.namespaces import RDF, RDFS

logger = logging.getLogger(__name__)


def create_pattern_map(graph: Collection[Statement],
                       pattern: 'GraphPattern')\
    -> tuple[list[tuple[Statement, 'AssertionPattern']],
             list[tuple[Statement, 'AssertionPattern']],
             set[Statement]]:
    """ Create a mapping between the observations in the newly
        observed state graph and the components of the associated
        graph pattern. This first tries to map the subpatterns
        that are part of the nominal behaviour and will next try
        to map the remaining observations to the subpattern
        candidates.

    :param graph: [TODO:description]
    :param pattern: [TODO:description]
    :return: [TODO:description]
    """
    # find pairs of assertions and associated assertion patterns
    pattern_components = list(pattern.structure.values())
    assertion_ap_pairs, unmatched\
        = match_assertions_to_patterns(graph, pattern_components)

    # find pairs for assertion patterns under consideration
    assertion_uc_pairs = list()
    if len(pattern._under_consideration) > 0:
        pattern_components = list(pattern._under_consideration.values())
        assertion_uc_pairs, unmatched\
            = match_assertions_to_patterns(unmatched, pattern_components)

    return assertion_ap_pairs, assertion_uc_pairs, unmatched


def match_assertions_to_patterns(
        graph: Collection[Statement],
        aPatterns: Collection['AssertionPattern'])\
                -> tuple[list[tuple[Statement, 'AssertionPattern']],
                         set[Statement]]:
    """ Find pairs of assertions with associated assertion patterns
        by first trying to match on relations, iff unique, and
        else by weakly comparing and finally strongly comparing
        the relation-object pairs.

    :param assertions: [TODO:description]
    :param assertionPatterns: [TODO:description]
    :return: [TODO:description]
    """
    # count relations of both inut sets combined
    relation_count = Counter([a.predicate for a in graph])
    relation_count.update([ap.relation for ap in aPatterns])

    # match assertions to their pattern
    remainder = set()
    assertion_ap_pairs = list()
    assertion_ap_conflicts = list()
    for assertion in graph:
        matches = list()
        if relation_count[assertion.predicate] == 2:
            # unambiguous match
            matches = [ap for ap in aPatterns
                       if ap.relation == assertion.predicate]
        elif relation_count[assertion.predicate] > 2:
            # ambiguous match
            for ap in aPatterns:
                if ap.strong_match(assertion):
                    # match on relation-object value
                    matches.append(ap)
                elif ap.weak_match(assertion):
                    # match on relation-object type
                    matches.append(ap)
        else:  # relation occurs exactly once
            # no pairing relations
            continue

        if len(matches) > 1:
            assertion_ap_conflicts.append((assertion, matches))
        elif len(matches) == 1:  # exact one match found
            assertion_ap_pairs.append((assertion, matches[-1]))
        else:  # no matches
            remainder.add(assertion)

    # conflict resolution: multiple matches per assertion
    if len(assertion_ap_conflicts) > 0:
        # TODO: implement some conflict resolution
        for assertion, _ in assertion_ap_conflicts:
            logger.debug(f"Matches multiple assertion patterns: {assertion}")

    return assertion_ap_pairs, remainder


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


def infer_class(resource: IRIRef, graph: Collection[Statement]) -> IRIRef:
    """ Infer class of provided resource by looking for associated
        type declaration. Defaults to rdfs:Resource if no such
        declaration can be found.

    :param resource: [TODO:description]
    :param graph: [TODO:description]
    :return: [TODO:description]
    """
    rdf_type = RDF + 'type'
    for assertion in graph:
        if assertion.subject == resource\
          and assertion.predicate == rdf_type\
          and isinstance(assertion.object, IRIRef):
            return assertion.object

    return RDFS + 'Resource'

#!/usr/bin/env python

import argparse
from datetime import datetime
import logging
import json
import os
from typing import Any
import tomllib

import numpy as np

from gladoss.core.utils import init_rng, gen_id


logger = logging.getLogger(__name__)


FILE_DIR = os.path.dirname(__file__)
FILENAME_CONF = "dummy-data.toml"
FILENAME_DATA = "dummy-data.json"

XSD_NS = "http://www.w3.org/2001/XMLSchema#"


def gen_entities(rng: np.random.Generator, conf: dict[str, Any],
                 samplesize: int, namespace: str)\
                         -> tuple[list[str], list[bool]]:
    """ Generate IRIRefs.

    :param rng: [TODO:description]
    :param changes_every: [TODO:description]
    :param samplesize: [TODO:description]
    :param namespace: [TODO:description]
    :return: [TODO:description]
    """
    out = list()

    # provided or randomly generated value
    value = '<' + namespace + conf.get('value', gen_id(rng)) + '>'

    # anomaly injection
    # this only makes sense if the value is set to static
    anomaly_every = conf.get('anomaly_every')
    anomaly_duration = conf.get('anomaly_duration', 1)

    # track anomalies
    anomaly_mask = list()

    changes_every = conf.get('changes_every', 1)
    for i in range(1, samplesize+1):
        value_new = None
        anomaly = False
        if anomaly_every is not None\
                and i >= anomaly_every\
                and i % anomaly_every <= (anomaly_duration-1):
            value_new = '<' + namespace + gen_id(rng) + '>'

            anomaly = True
        elif changes_every is not None and i % changes_every == 0:
            id_str = gen_id(rng)
            value_new = '<' + namespace + f"{id_str}" + '>'

        if value_new is None:
            out.append(value)
        else:
            out.append(value_new)

        anomaly_mask.append(anomaly)

    if conf.get('sort', False):
        # sort in natural order
        out, anomaly_mask\
                = (list(t) for t in zip(*sorted(zip(out, anomaly_mask))))

    return (out, anomaly_mask)


def gen_random_sentence(rng: np.random.Generator) -> str:
    """ Generate a random sentence by stacking characters.

    :param rng: [TODO:description]
    :return: [TODO:description]
    """
    a, z = 97, 122
    size = rng.integers(10, 100)  # random length

    # generate characters
    s = np.array([chr(i) for i in rng.integers(low=a, high=z, size=size,
                                               endpoint=True)])

    # add white space
    padding = 3
    ws = rng.integers(padding, size-padding,
                      size=rng.integers(size//10, size//5))

    s[ws] = ' '

    # make sentence
    s[0] = s[0].upper()
    s = ''.join(s)
    s += '.'

    return s


def gen_anomaly(rng: np.random.Generator, dtype: str,
                v_from: Any, v_to: Any, multiplier: float) -> Any:
    """ Generate one literal value of a certain datatype out
        of distribution.

    :param rng: [TODO:description]
    :param dtype: [TODO:description]
    :param v_from: [TODO:description]
    :param v_to: [TODO:description]
    :param multiplier: [TODO:description]
    :return: [TODO:description]
    """
    if dtype == "string":
        # anomalies within dynamic string content isn't supported by the
        # anomaly detector, so this only makes sense if the value is set
        # as static.
        return gen_random_sentence(rng)
    elif dtype == "float":
        v_delta = v_to - v_from
        if rng.random() >= 0.5:
            mu = (multiplier * (v_to + v_delta)) / 2
        else:
            mu = (multiplier * (v_from - v_delta)) / 2
        sigma = (v_to - v_from) / 6

        v = sigma * rng.standard_normal() + mu

        return float(v)
    elif dtype == "int":
        return int(gen_value(rng, "float", v_from, v_to))
    elif dtype == "gYear":
        return int(gen_value(rng, "float", v_from, v_to))
    elif dtype == "dateTime":
        v_from = datetime.fromisoformat(v_from).timestamp()
        v_to = datetime.fromisoformat(v_to).timestamp()

        unix_timestamp = gen_value(rng, "float", v_from, v_to)

        return datetime.fromtimestamp(unix_timestamp).isoformat()
    else:
        return ""


def gen_value(rng: np.random.Generator, dtype: str,
              v_from: Any, v_to: Any) -> Any:
    """ Generate one literal value of a certain datatype.

    :param rng: [TODO:description]
    :param dtype: [TODO:description]
    :param v_from: [TODO:description]
    :param v_to: [TODO:description]
    :return: [TODO:description]
    """
    if dtype == "string":
        return gen_random_sentence(rng)
    elif dtype == "float":
        mu = (v_from + v_to) / 2
        sigma = (v_to - v_from) / 6

        v = sigma * rng.standard_normal() + mu
        while v < v_from or v > v_to:
            v = sigma * rng.standard_normal() + mu

        return float(v)
    elif dtype == "int":
        return int(gen_value(rng, "float", v_from, v_to))
    elif dtype == "gYear":
        return int(gen_value(rng, "float", v_from, v_to))
    elif dtype == "dateTime":
        v_from = datetime.fromisoformat(v_from).timestamp()
        v_to = datetime.fromisoformat(v_to).timestamp()

        unix_timestamp = gen_value(rng, "float", v_from, v_to)

        return datetime.fromtimestamp(unix_timestamp).isoformat()
    else:
        return ""


def gen_literals(rng: np.random.Generator, conf: dict[str, Any],
                 samplesize: int) -> tuple[list[str], list[bool]]:
    """ Generate a collection of literals.

    :param rng: [TODO:description]
    :param conf: [TODO:description]
    :param samplesize: [TODO:description]
    :return: [TODO:description]
    """
    out = list()

    # range and datatype
    v_from, v_to = conf.get('from'), conf.get('to')
    dtype = conf.get('type', 'string')

    # provided or randomly generated value
    value = conf.get('value')
    if value is None:
        value = str(gen_value(rng, dtype, v_from, v_to))
        value += f"^^<{XSD_NS}{dtype}>"

    # anomaly injection
    anomaly_every = conf.get('anomaly_every')
    anomaly_duration = conf.get('anomaly_duration', 1)
    anomaly_mp = conf.get('anomaly_multiplier')

    # track anomalies
    anomaly_mask = list()

    changes_every = conf.get('changes_every')
    for i in range(1, samplesize+1):
        value_new = None
        anomaly = False
        if anomaly_every is not None\
                and i >= anomaly_every\
                and i % anomaly_every <= (anomaly_duration-1):
            value_new = str(gen_anomaly(rng, dtype, v_from, v_to, anomaly_mp))
            value_new = f"\"{value_new}\"^^<{XSD_NS}{dtype}>"

            anomaly = True
        elif changes_every is not None and i % changes_every == 0:
            value_new = str(gen_value(rng, dtype, v_from, v_to))
            value_new = f"\"{value_new}\"^^<{XSD_NS}{dtype}>"

        if value_new is None:
            out.append(value)
        else:
            out.append(value_new)

        anomaly_mask.append(anomaly)

    if conf.get('sort', False):
        # sort in natural order
        out, anomaly_mask\
                = (list(t) for t in zip(*sorted(zip(out, anomaly_mask))))

    return (out, anomaly_mask)


def mknodes(rng: np.random.Generator, conf: list[dict[str, Any]],
            namespace: str, samplesize: int)\
        -> dict[str, tuple[list[str], list[bool]]]:
    """ Generate node values.

    :param rng: [TODO:description]
    :param conf: [TODO:description]
    :param namespace: [TODO:description]
    :param samplesize: [TODO:description]
    :return: [TODO:description]
    """
    out = dict()
    for node in conf:
        name = node['name']
        if node['type'] == "IRIRef":
            out[name] = gen_entities(rng, node, samplesize, namespace)
        else:  # literal
            out[name] = gen_literals(rng, node, samplesize)

    return out


def expandPrefixes(pattern: str, prefixes: dict[str, str]) -> str:
    """ Expand prefixes in pattern to complete IRIs following the
        N-Triple specification.

    :param pattern: [TODO:description]
    :param prefixes: [TODO:description]
    :return: [TODO:description]
    :raises Exception: [TODO:description]
    """
    pattern_exp = list()
    for line in pattern.splitlines():
        line_split = line.strip().split(' ')

        if len(line_split) == 3 and '.' in line_split[-1]:
            # separate final dot from object
            line_split[-1] = line_split[-1][:-1]
            line_split.append('.')
        elif len(line_split) == 4 and line_split[-1] == '.':
            # valid triple pattern
            for i in range(3):  # omit final dot
                value = line_split[i]
                if ':' not in value:
                    # no need to expand
                    continue

                # expand prefix
                prefix, name = value.split(':')
                line_split[i] = '<' + prefixes[prefix] + name + '>'
        else:
            raise Exception("Prefix expansion failed")

        pattern_exp.append(' '.join(line_split))

    pattern_exp = '\n'.join(pattern_exp)

    return pattern_exp


def mkdata(label: str, pattern: str, prefixes: dict[str, str],
           values: dict[str, tuple[list[str], list[bool]]], samplesize: int)\
        -> list[dict[str, str]]:
    """ Combine generated data with provided pattern by replacing
        the variables with their bindings.

    :param pattern: [TODO:description]
    :param anchors: [TODO:description]
    :param variables: [TODO:description]
    :param samplesize: [TODO:description]
    :return: [TODO:description]
    """
    pattern = pattern.strip()
    pattern = expandPrefixes(pattern, prefixes)

    out = list()
    for i in range(samplesize):
        g = pattern

        anomaly = False
        for var, (binding_lst, anomaly_msk) in values.items():
            g = g.replace('?'+var, binding_lst[i])

            if anomaly_msk[i]:
                anomaly = True

        out.append({'label': label, 'anomaly': anomaly, 'data': g})

    return out


def main(conf: dict[str, Any], flags: argparse.Namespace)\
        -> list[list[dict[str, str | list[str]]]]:
    """ Generate samples for each entry in the configuration.

    :param conf: the configuration
    :param flags: user-specified parameters
    :return: the generates samples for all entries
    """
    rng = init_rng(flags.seed)

    data = list()
    samplesize = flags.samplesize
    for entry in conf['data']:
        namespace = entry['namespace']
        nodes = entry['node']

        # generate graph label
        label = gen_id(rng)

        # generate values
        values = mknodes(rng, nodes, namespace, samplesize)

        # generate data
        pattern = entry['pattern']
        prefixes = entry['prefixes']
        samples = mkdata(label, pattern, prefixes, values, samplesize)

        data.append(samples)

    return data


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="The path to the configuration "
                        + "file (toml)", default=os.path.join(FILE_DIR,
                                                              FILENAME_CONF))
    parser.add_argument("-n", "--samplesize", help="The number of samples to "
                        + "generate", default=400, type=int)
    parser.add_argument("-o", "--output", help="The path to the output "
                        + "file (json)", default=os.path.join(FILE_DIR,
                                                              FILENAME_DATA))
    parser.add_argument("--seed", help="Seed for random number generator",
                        default=None, type=int)
    parser.add_argument("--verbose", "-v", help="Show debug messages in "
                        + "console.", action='count', default=0)
    flags = parser.parse_args()

    # set log level
    log_level = logging.NOTSET
    if flags.verbose >= 2:
        log_level = logging.DEBUG
    elif flags.verbose == 1:
        log_level = logging.INFO
    else:
        log_level = logging.WARNING

    logging.basicConfig(level=log_level,
                        format='[%(asctime)s] [%(levelname)s] %(filename)s '
                               '- %(message)s')

    conf = dict()
    logger.debug("Reading configuration from %s" % flags.config)
    with open(flags.config, 'rb') as f:
        conf = tomllib.load(f)

    data = main(conf, flags)
    logger.debug("Writing data to file %s" % flags.output)
    with open(flags.output, 'w') as f:
        json.dump(data, f, indent=4)

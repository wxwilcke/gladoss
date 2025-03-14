#!/usr/bin/env python

import argparse
from datetime import datetime
import logging
import json
import os
from typing import Any
import tomllib

import numpy as np
import scipy as sp

from gladoss.core.utils import init_rng


# TODO: Add ability to inject anomalies into the generated data


logger = logging.getLogger(__name__)


FILE_DIR = os.path.dirname(__file__)
FILENAME_CONF = "dummy-data.toml"
FILENAME_DATA = "dummy-data.json"

XSD_NS = "http://www.w3.org/2001/XMLSchema#"


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


def mkstatic(rng: np.random.Generator, conf: list[dict[str, Any]],
             namespace: str) -> dict[str, str]:
    """ Generate data which keeps the same value throughout the
        entire simulated series.

    :param rng: [TODO:description]
    :param conf: [TODO:description]
    :param namespace: [TODO:description]
    :return: [TODO:description]
    """
    out = dict()
    for node in conf:
        name = node['name']
        if node['type'] == "IRIRef":
            value = namespace
            if 'value' in node.keys():
                value += node['value']
            else:
                id_str = gen_id(rng)
                value += f"{id_str}"

            value = f"<{value}>"
        else:  # Literal
            value = '\"' + node['value'] + '\"'
            value += f"^^<{XSD_NS}{node['type']}>"

        out[name] = value

    return out


def gen_entities(rng: np.random.Generator, changes_every: int, samplesize: int,
                 namespace: str) -> list[str]:
    """ Generate IRIRefs that can change over time.

    :param rng: [TODO:description]
    :param changes_every: [TODO:description]
    :param samplesize: [TODO:description]
    :param namespace: [TODO:description]
    :return: [TODO:description]
    """
    out = list()

    id_str = gen_id(rng)
    value = namespace + f"{id_str}"
    for i in range(1, samplesize+1):
        if i % changes_every == 0:
            id_str = gen_id(rng)
            value = namespace + f"{id_str}"

        value = f"<{value}>"
        out.append(value)

    return out


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
        sigma = (v_to - v_from) / 4

        v = sp.stats.Normal(mu=mu, sigma=sigma).sample(shape=1, rng=rng)[0]
        return float(v)
    elif dtype == "int":
        return int(gen_value(rng, "float", v_from, v_to))
    elif dtype == "dateTime":
        v_from = datetime.fromisoformat(v_from).timestamp()
        v_to = datetime.fromisoformat(v_to).timestamp()

        unix_timestamp = gen_value(rng, "float", v_from, v_to)

        return datetime.fromtimestamp(unix_timestamp).isoformat()
    else:
        return ""


def gen_literals(rng: np.random.Generator, conf: dict[str, Any],
                 samplesize: int) -> list[str]:
    """ Generate a collection of literals which change over time.

    :param rng: [TODO:description]
    :param conf: [TODO:description]
    :param samplesize: [TODO:description]
    :return: [TODO:description]
    """
    out = list()

    v_from, v_to = conf['from'], conf['to']
    dtype = conf['type']

    value = str(gen_value(rng, dtype, v_from, v_to))
    value += f"^^<{XSD_NS}{dtype}>"
    for i in range(1, samplesize+1):
        if i % conf['changes_every'] == 0:
            value = "\"" + str(gen_value(rng, dtype, v_from, v_to)) + "\""
            value += f"^^<{XSD_NS}{dtype}>"

        out.append(value)

    if dtype == "dateTime":
        # sort time in chronical order
        out = sorted(out)

    return out


def mkdynamic(rng: np.random.Generator, conf: list[dict[str, Any]],
              namespace: str, samplesize: int)\
        -> dict[str, list[str]]:
    """ Generate changing values.

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
            out[name] = gen_entities(rng, node['changes_every'],
                                     samplesize, namespace)
        else:  # literal
            out[name] = gen_literals(rng, node, samplesize)

    return out


def mkdata(pattern: str, anchors: dict[str, str],
           variables: dict[str, list[str]], samplesize: int)\
                   -> list[dict[str, str]]:
    """ Combine generated data with provided pattern by replacing
        the variables with their bindings.

    :param pattern: [TODO:description]
    :param anchors: [TODO:description]
    :param variables: [TODO:description]
    :param samplesize: [TODO:description]
    :return: [TODO:description]
    """
    out = list()
    for i in range(samplesize):
        g = pattern
        for var, binding in anchors.items():
            g = g.replace('?'+var, binding)

        for var, binding_lst in variables.items():
            g = g.replace('?'+var, binding_lst[i])

        out.append({"data": g.strip()})

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
        dynamic_nodes = entry['dynamic']
        static_nodes = entry['static']
        namespace = entry['namespace']

        # generate values
        anchors = mkstatic(rng, static_nodes, namespace)
        variables = mkdynamic(rng, dynamic_nodes, namespace, samplesize)

        # generate data
        pattern = entry['pattern']
        samples = mkdata(pattern, anchors, variables, samplesize)

        # add anchers
        anchors_value = list(anchors.values())
        for sample in samples:
            sample.update({"anchors": anchors_value})

        data.append(samples)

    return data


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="The path to the configuration "
                        + "file (toml)", default=os.path.join(FILE_DIR,
                                                              FILENAME_CONF))
    parser.add_argument("-n", "--samplesize", help="The number of samples to "
                        + "generate", default=100, type=int)
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

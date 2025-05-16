#!/usr/bin/env python

import argparse
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime
import logging
from queue import Queue
import signal
from threading import Event
from types import SimpleNamespace
from typing import Collection, Optional

import numpy as np
from gladoss.core.connector import Connector
from rdf.graph import Statement

from gladoss.adaptors.adaptor import Adaptor
from gladoss.data.backup import BackupManager
from gladoss.data.utils import create_namespace_subset, timeSpanArg
from gladoss.core.pattern import (GraphPattern, PatternVault,
                                  create_graph_pattern, update_graph_pattern)
from gladoss.core.validator import ValidationReport, validate_state_graph
from gladoss.core.utils import import_class, init_rng


logger = logging.getLogger(__name__)

_ADAPTORS = {
        "dummy": ["dummy", "DummyAdaptor"],
        "knowledge_engine": ["knowledge_engine", "KE_Adaptor"]
        }


def signal_handler(signum, frame):
    """ Wait for a keyboard Interrupt after which
        to set the signal to gracefully terminate
        all connections.

    :param signum [TODO:type]: [TODO:description]
    :param frame [TODO:type]: [TODO:description]
    """
    signal.signal(signum, signal.SIG_IGN)
    logger.info("Received Keyboard Interrupt")

    global controller
    controller.set()


def create_validation_report(pattern: GraphPattern,
                             facts: Collection[Statement],
                             econf: SimpleNamespace) -> ValidationReport:
    report = validate_state_graph(pattern, facts, econf)

    # TODO Placeholder
    report = ValidationReport(pattern=pattern, facts=facts,
                              timestamp=datetime.now(),
                              grade=ValidationReport.Grade.PASSED,
                              metadata={})

    return report


def publish_validation_report(adaptor: Adaptor, report: ValidationReport):
    pass


def listener(connector: Connector, q: Queue) -> None:
    """ Listen on an endpoint for new messages. Queue
        these upon arrival. This operation is thread safe.

    :param connector: [TODO:description]
    :param q: [TODO:description]
    """
    for graph, graph_id in connector.listen():
        q.put((graph, graph_id))


def process_observation(rng: np.random.Generator, pv: PatternVault,
                        graph: Collection[Statement],
                        graph_id: str, pconf: SimpleNamespace,
                        econf: SimpleNamespace)\
        -> ValidationReport:
    """ Process an incoming message by finding the associated graph
        pattern, then evaluating the message with respect to this
        pattern, and, if OK, use the message to update the pattern.
        A new pattern is created if the message identifier is unknown,
        and a validation report is created and returned upon completion.

    :param rng: [TODO:description]
    :param pv: [TODO:description]
    :param graph: [TODO:description]
    :param graph_id: [TODO:description]
    :param config: [TODO:description]
    """
    gpattern = pv.find_associated_graph_pattern(graph_id)
    if gpattern is None:
        logger.debug(f"Associated pattern not found: {graph}")
        gpattern = create_graph_pattern(rng=rng, graph=graph,
                                        graph_id=graph_id,
                                        threshold=pconf.pattern_threshold,
                                        decay=pconf.pattern_decay)
        pv.add_graph_pattern(gpattern)

    report = create_validation_report(gpattern, graph, econf)
    if report.status_code == ValidationReport.StatusCode.ERROR:
        logger.warning("Unable to validate observed state graph")

    if report.status_code != ValidationReport.StatusCode.FAILED:
        # either the state graph passed the validation check
        # or a (still) non-critical deviation has been detected
        gpattern_upd = update_graph_pattern(rng, gpattern, graph, pconf)
        pv.update_graph_pattern(gpattern_upd)

    return report


def main(rng: np.random.Generator, adaptor_cls: Adaptor,
         flags: argparse.Namespace, cconf: SimpleNamespace,
         pconf: SimpleNamespace, econf: SimpleNamespace) -> None:
    logger.info("Initiating Program")

    # setup adaptor to manage communication and to translate incoming messages
    adaptor = adaptor_cls(controller=controller,  # type: ignore
                          config=cconf)

    # initiate pattern vault which will manage and track patterns over time
    pv = PatternVault()

    # setup backup manager to periodically write the pattern vault to disk
    bckmgr = BackupManager(pv, flags.backup_path, flags.backup_interval)
    bckmgr.enable_auto_backup()

    with ThreadPoolExecutor(flags.max_cores) as executor:
        q = Queue()  # queue jobs here

        # listen to all endpoints in parallel
        jobs_active = {executor.submit(listener, connector, q)
                       for connector in adaptor.connectors}
        while len(jobs_active) > 0:
            # check which jobs have been completed
            jobs_completed, _ = wait(jobs_active, return_when=FIRST_COMPLETED)

            while not q.empty():
                # process incoming messages: spawn a new thread for each
                job = q.get()

                graph, graph_id = job
                job_fs = executor.submit(process_observation, rng, pv,
                                         graph, graph_id, pconf, econf)

                jobs_active.add(job_fs)  # type: ignore

            # process output of completed jobs
            for job_fs in jobs_completed:
                try:
                    report = job_fs.result()  # type: ValidationReport

                    # send report if requested by the report level
                    if report.status_code < econf.report_level:
                        publish_validation_report(adaptor, report)
                except Exception as e:
                    logger.error(f"Job execution raised execption: {e}")
                finally:
                    # remove finished jobs
                    jobs_active.remove(job_fs)

    bckmgr.disable_auto_backup()
    bckmgr.create_backup()  # emergency backup

    logger.info("Waiting on connections to close...")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="GLADoSS",
        description="Graph-based Live Anomaly Detection on Semantic Streams",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        epilog="The development of this program has been funded by HEDGE-IoT")
    parser.add_argument("--backup_interval", help="Intervals between backups. "
                        + "Expects the input to be an integer followed by 'H'"
                        + ", 'D', or 'W', denoting hours, days, or weeks.",
                        type=timeSpanArg, default=None)
    parser.add_argument("--backup_path", help="Directory to write backups to",
                        type=str, default="/tmp/")  # FIXME: change
    parser.add_argument("--max_cores", help="Maximum number of allowed cpu "
                        "cores to utilise.", default=None, type=int)
    parser.add_argument("--seed", help="Seed for random number generator "
                        + "(optional)", type=int, default=None)
    parser.add_argument("--verbose", "-v", help="Show debug messages in "
                        + "console.", action='count', default=0)

    parser_comm = parser.add_argument_group('Communication Settings')
    parser_comm.add_argument("adaptor", help="Adaptor appropriate for "
                             + "endpoint", choices=list(_ADAPTORS.keys()),
                             type=str, nargs=1)
    parser_comm.add_argument("--endpoint", help="HTTP address to listen to",
                             default="http://127.0.0.1:8000", type=str)
    parser_comm.add_argument("--continuous", help="Keep listening for changes"
                             " in the response, irrespective of response "
                             "status", default=False, action="store_true")
    parser_comm.add_argument("--retries", help="Number of retries on error",
                             default=3, type=int)
    parser_comm.add_argument("--retry_delay", help="Number of seconds to wait "
                             + "before retrying after the occurrence of an "
                             + "error", default=30, type=int)
    parser_comm.add_argument("--return_receipt", help="Send acknowledgement "
                             + "to sender upon reception of message.",
                             action='store_true', default=False)
    parser_comm.add_argument("--request_delay", help="Number of seconds to "
                             + "wait between polling the server.", default=0.5,
                             type=int)

    parser_patt = parser.add_argument_group('Pattern Recognition Settings')
    parser_patt.add_argument("--pattern_decay", help="Number of epoch passed "
                             "until an absent pattern component is forgotten. "
                             "A negative value disables this feature "
                             "entirely.", type=int, default=-1)
    parser_patt.add_argument("--pattern_threshold", help="Number of epoch "
                             "passed until an new pattern component is "
                             "added to the pattern. A negative value disables"
                             " this feature entirely.", type=int, default=-1)
    parser_patt.add_argument("--pattern_resolution", help="Number of "
                             "significant figures to take into account when "
                             "evaluating a new sample. A negative value "
                             "disables this feature.", type=int, default=-1)

    parser_eval = parser.add_argument_group('Anomaly Detection Settings')
    parser_eval.add_argument("--significance_level_critical",
                             help="Significance level (alpha) for the test "
                             "statistic. A p-value less than this level will "
                             "trigger a critical warning.", type=float,
                             default=0.05, dest='alpha_crit')
    parser_eval.add_argument("--significance_level_suspicious",
                             help="Significance level (alpha) for the test "
                             "statistic. A p-value less than this level will "
                             "trigger a warning.", type=float, default=0.10,
                             dest='alpha_susp')
    parser_eval.add_argument("--evaluate_structure", help="Evaluate the "
                             "structure of the observed state graph against "
                             "the associated graph pattern.", type=bool,
                             action=argparse.BooleanOptionalAction,
                             default=True)
    parser_eval.add_argument("--evaluate_data", help="Evaluate the "
                             "data of the observed state graph against "
                             "the associated graph pattern.", type=bool,
                             action=argparse.BooleanOptionalAction,
                             default=True)
    parser_eval.add_argument("--samplesize", help="Number of samples to draw "
                             "from the population and to evaluate against "
                             "the distribution underlying that population. "
                             "Samples are drawn in chronologically reversed "
                             "order such that the result contains the most "
                             "recent n samples.", type=int, default=50)
    parser_eval.add_argument("--samplegap", help="Number of samples to skip "
                             "between the population and test sample when "
                             "sorted in chronological ordered. This can "
                             "create a stronger distinction between "
                             "distributions.", type=int, default=10)
    parser_eval.add_argument("--match_cwa", help="If enabled, employ the "
                             "Closed World Assumption during the evaluation "
                             "of an observed state graph: expected yet "
                             "missing triples will now trigger a warning.",
                             type=bool, action='store_true', default=False)
    parser_eval.add_argument("--match_exact", help="If enabled, any missing "
                             "or extra triples in the observed state graph "
                             "will trigger a warning.", type=bool,
                             action='store_true', default=False)
    parser_eval.add_argument("--report_level", help="Reports of which level "
                             "and below will be send to the endpoint. "
                             "Critical warnings (1), suspicious warnings (2), "
                             "successful validations (3), or validation "
                             "errors (4). Value zero (0) will disable report "
                             "publications.", type=int, default=2)

    flags = parser.parse_args()

    # create subsets per function
    cconf = create_namespace_subset(flags, ['adaptor', 'endpoint',
                                            'continuous', 'retries',
                                            'retry_delay', 'request_delay',
                                            'return_receipt'])
    pconf = create_namespace_subset(flags, ['pattern_decay',
                                            'pattern_threshold',
                                            'pattern_resolution'])
    econf = create_namespace_subset(flags, ['significance_level_critical',
                                            'significance_level_suspicious',
                                            'evaluate_structure',
                                            'evaluate_data',
                                            'samplesize',
                                            'samplegap',
                                            'match_cwa',
                                            'match_exact',
                                            'report_level'])

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

    # register SIGINT signal handler
    global controller
    controller = Event()

    signal.signal(signal.SIGINT, signal_handler)

    # set random number generator
    rng = init_rng(flags.seed)

    # import specified adaptor
    adaptor = import_class(_ADAPTORS, flags.adaptor)

    # start main loop
    main(rng, adaptor, flags, cconf, pconf, econf)

#!/usr/bin/env python

import argparse
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime
import functools
import logging
from queue import Queue
import signal
from threading import Event
import threading
from types import SimpleNamespace
from typing import Callable, Collection

import numpy as np
from gladoss.core.connector import Connector
from rdf.graph import Statement

from gladoss.adaptors.adaptor import Adaptor
from gladoss.data.backup import BackupManager
from gladoss.data.utils import create_namespace_subset, timeSpanArg
from gladoss.core.pattern import (AssertionPattern, GraphPattern, PatternVault,
                                  create_graph_pattern, update_graph_pattern)
from gladoss.core.validator import ValidationReport, validate_state_graph
from gladoss.core.utils import (create_pattern_map, gen_id, import_class,
                                init_rng)


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


def publish_validation_report(adaptor: Adaptor, report: ValidationReport,
                              mkid: Callable) -> bool:
    """ Convert the validation report to RDF graph format and publish
        the result via the adaptor.

    :param adaptor: [TODO:description]
    :param report: [TODO:description]
    :param mkid: [TODO:description]
    :return: [TODO:description]
    """
    # represent validation report as graph
    logger.info(f"Creating validation publication ({report.pattern._id})")
    report_graph = report.to_graph(mkid)

    # publish report to endpoint
    logger.info(f"Publishing validation report ({report.pattern._id})")
    success = adaptor.publish_report(report.pattern._id, report_graph)

    return success


def create_validation_report(rng: np.random.Generator,
                             pattern: GraphPattern,
                             graph: Collection[Statement],
                             pattern_map: tuple[list[tuple[Statement,
                                                           AssertionPattern]],
                                                list[tuple[Statement,
                                                           AssertionPattern]],
                                                set[Statement]],
                             econf: SimpleNamespace) -> ValidationReport:
    """ Generate a validation report for the observed state graph given
        the associated graph pattern. This will start the validation
        procedure.

    :param rng: [TODO:description]
    :param pattern: [TODO:description]
    :param graph: [TODO:description]
    :param econf: [TODO:description]
    :return: [TODO:description]
    """
    try:
        if pattern._t > 0:
            # skip new patterns
            logger.info(f"Creating validation report ({pattern._id})")

        report = validate_state_graph(rng, pattern, graph, pattern_map, econf)
    except Exception as err:
        logger.error(err)

        # convert to simpler form for validation report
        assertion_ap_pairs, _, _ = pattern_map
        apa_map = {ap._id: a for a, ap in assertion_ap_pairs}

        # create validation report without technical detaiks (which are logged)
        status_msg = "Validation Malfunction"
        status_msg_long = "An exception occurred during the evaluation of "\
                          f"the observed state graph with ID '{pattern._id}.'"
        status_code = ValidationReport.StatusCode.ERROR
        report = ValidationReport(pattern=pattern, graph=graph,
                                  timestamp=datetime.now(),
                                  apa_map=apa_map,
                                  status_code=status_code,
                                  status_msg_lst=[(status_msg,
                                                   status_msg_long,
                                                   status_code)])

    return report


def listener(connector: Connector, q: Queue) -> None:
    """ Listen on an endpoint for new messages. Queue
        these upon arrival. This operation is thread safe.

    :param connector: [TODO:description]
    :param q: [TODO:description]
    """
    for graph_id, graph in connector.listen():
        q.put((graph_id, graph))


def process_observation(rng: np.random.Generator, mkid: Callable,
                        pv: PatternVault, graph: Collection[Statement],
                        graph_id: str, pconf: SimpleNamespace,
                        econf: SimpleNamespace, thread_id: int)\
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
    threading.current_thread().name = f"Thread-{thread_id}"
    logger.info(f"Received new graph message ({graph_id})")

    pattern = pv.find_associated_graph_pattern(graph_id)
    if pattern is None:
        logger.debug(f"Associated pattern not found ({graph_id})")
        pattern = create_graph_pattern(mkid=mkid, graph=graph,
                                       graph_id=graph_id,
                                       threshold=pconf.pattern_threshold,
                                       decay=pconf.pattern_decay)
        pv.add_graph_pattern(pattern)
    else:
        logger.debug(f"Associated pattern found ({graph_id})")

    pattern_map = create_pattern_map(graph, pattern)
    report = create_validation_report(rng, pattern, graph, pattern_map, econf)
    if report.status_code in [ValidationReport.StatusCode.NOMINAL,
                              ValidationReport.StatusCode.SUSPICIOUS,
                              ValidationReport.StatusCode.NODATA]:
        if pattern._t > 0:
            # skip new patterns
            logger.info(f"Graph passed validation ({graph_id})")

            # either the state graph passed the validation check
            # or a non-critical deviation has been detected
            gpattern_upd = update_graph_pattern(mkid, pattern, graph,
                                                pattern_map, pconf)
            pv.update_graph_pattern(gpattern_upd)
    else:
        logger.info(f"Graph failed validation ({graph_id})")

    return report


def main(rng: np.random.Generator, adaptor_cls: Adaptor,
         flags: argparse.Namespace, cconf: SimpleNamespace,
         pconf: SimpleNamespace, econf: SimpleNamespace) -> None:
    """ Initalise adaptor and one or more connections, the pattern vault,
        and backup manager, and start several parallel jobs which are to
        listen for new incoming messages from the connection(s). Once such
        a message has been received, a new thread will be spawned that
        further processes this message, returning a validation report upon
        completion. The report will be published to the endpoint if requested.

        The main loop of this procedure will continue indefinitely, only to
        stop upon a keyboard interrupt or a fatal connection error.

    :param rng: [TODO:description]
    :param adaptor_cls: [TODO:description]
    :param flags: [TODO:description]
    :param cconf: [TODO:description]
    :param pconf: [TODO:description]
    :param econf: [TODO:description]
    """
    logger.info("Initiating Program")

    # create callable to avoid importing numpy everywhere
    mkid = functools.partial(gen_id, rng)

    # setup adaptor to manage communication and to translate incoming messages
    adaptor = adaptor_cls(controller=controller,  # type: ignore
                          config=cconf)

    # initiate pattern vault which will manage and track patterns over time
    pv = PatternVault()

    # setup backup manager to periodically write the pattern vault to disk
    bckmgr = BackupManager(pv, flags.backup_path, flags.backup_interval)
    # bckmgr.enable_auto_backup()  # FIXME disabed for debugging

    thread_id = 1
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

                graph_id, graph = job
                job_fs = executor.submit(process_observation, rng, mkid, pv,
                                         graph, graph_id, pconf, econf,
                                         thread_id)

                jobs_active.add(job_fs)  # type: ignore

                # increase thread number
                thread_id += 1
                if thread_id % 99 == 0:
                    # reset to avoid huge numbers
                    thread_id = 1

            # process output of completed jobs
            for job_fs in jobs_completed:
                try:
                    # wait until a new report comes in
                    report = job_fs.result()
                    if report is None:
                        logger.debug("A listener job has ended")

                        continue

                    # publish report if requested by the report level
                    assert isinstance(report, ValidationReport)
                    if report.status_code >= econf.report_level:
                        publish_validation_report(adaptor, report, mkid)
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
                             type=str)
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
                             default=0.05, dest='alpha_critical')
    parser_eval.add_argument("--significance_level_suspicious",
                             help="Significance level (alpha) for the test "
                             "statistic. A p-value less than this level will "
                             "trigger a warning.", type=float, default=0.10,
                             dest='alpha_suspicious')
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
    parser_eval.add_argument("--grace_period", help="Number of updates to "
                             "process per assertion before evaluating the "
                             "assertion during the validation procedure. "
                             "This can be regarded as the training time.",
                             type=int, default=100)
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
                             action='store_true', default=False)
    parser_eval.add_argument("--match_exact", help="If enabled, any missing "
                             "or extra triples in the observed state graph "
                             "will trigger a warning.",
                             action='store_true', default=False)
    parser_eval.add_argument("--report_level", help="Reports of equal level "
                             "and higher will be published to the endpoint: "
                             "NOMINAL behaviour (0), generic ERRORS (1), "
                             "INSUFFICIENT DATA (2), INCONSISTENCIES (3), "
                             "SUSPICIOUS warnings (4), and CRITICAL "
                             "warnings (5)",
                             type=int, default=5)

    flags = parser.parse_args()

    # create subsets per function
    cconf = create_namespace_subset(flags, ['adaptor', 'endpoint',
                                            'continuous', 'retries',
                                            'retry_delay', 'request_delay',
                                            'return_receipt'])
    pconf = create_namespace_subset(flags, ['pattern_decay',
                                            'pattern_threshold',
                                            'pattern_resolution'])
    econf = create_namespace_subset(flags, ['alpha_critical',
                                            'alpha_suspicious',
                                            'evaluate_structure',
                                            'evaluate_data',
                                            'grace_period',
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
                        format='[%(asctime)s] [%(levelname)s] [%(threadName)s]'
                               ' %(filename)s - %(message)s')

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

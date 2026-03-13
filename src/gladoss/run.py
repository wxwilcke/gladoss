#!/usr/bin/env python

import argparse
from datetime import datetime
import functools
import logging
import os
from pathlib import Path
from queue import Queue
import signal
from threading import Event, RLock
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
                                init_rng, list_classes)


logger = logging.getLogger(__name__)
ROOT_PATH = Path(__file__).parent
ADAPTER_ENV_NAME = "GLADOSS_ADAPTOR_DIRECTORY"


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

    :param pattern: [TODO:description]
    :param graph: [TODO:description]
    :param econf: [TODO:description]
    :return: [TODO:description]
    """
    try:
        if pattern._t >= econf.grace_period:
            logger.info(f"Creating validation report ({pattern._id})")
        else:
            logger.info("Within grace period: skipping validation "
                        f"({pattern._id})")
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


def process_graph(rng: np.random.Generator, mkid: Callable,
                  pv: PatternVault, graph: Collection[Statement],
                  graph_id: str, pconf: SimpleNamespace,
                  econf: SimpleNamespace, r: Queue):
    """ Process an incoming message by finding the associated graph
        pattern, then evaluating the message with respect to this
        pattern, and, if OK, use the message to update the pattern.
        A new pattern is created if the message identifier is unknown,
        and a validation report is created and returned upon completion.

    :param pv: [TODO:description]
    :param graph: [TODO:description]
    :param graph_id: [TODO:description]
    :param config: [TODO:description]
    """
    thread_id = threading.current_thread().name
    logger.info(f"Received new graph message ({graph_id})")

    pattern = pv.find_associated_graph_pattern(graph_id)
    if pattern is None:
        logger.debug(f"Associated pattern not found ({graph_id})")
        pattern = create_graph_pattern(mkid=mkid, graph=graph,
                                       graph_id=graph_id,
                                       threshold=pconf.pattern_threshold,
                                       decay=pconf.pattern_decay)

        logger.debug(f"Adding new pattern to pattern vault ({graph_id})")
        pv.add_graph_pattern(pattern)

        return  # no need to evaluate a graph on first sight

    logger.debug(f"Associated pattern found ({graph_id})")

    pattern_map = create_pattern_map(graph, pattern)
    report = create_validation_report(rng, pattern, graph, pattern_map, econf)
    if report.status_code in [ValidationReport.StatusCode.NOMINAL,
                              ValidationReport.StatusCode.NODATA]:
        if pattern._t >= econf.grace_period:
            logger.info(f"Graph passed validation ({graph_id})")

        # either the state graph passed the validation check
        # or a non-critical deviation has been detected
        gpattern_upd = update_graph_pattern(mkid, pattern, graph,
                                            pattern_map, pconf)
        pv.update_graph_pattern(gpattern_upd)
    else:
        logger.info(f"Graph failed validation ({graph_id})")

    r.put((thread_id, report))


def process_observation(rng: np.random.Generator, mkid: Callable,
                        pv: PatternVault, pconf: SimpleNamespace,
                        econf: SimpleNamespace, q: Queue, r: Queue) -> None:
    """ Process incoming messages by spawning a new thread on demand. This
        procedure should only be called by the manager, which itself should
        run on a thread different from the main thread to avoid blocking
        when waiting for a new observation to arrive. The manager can be
        told to terminate its pool of workers and itself by putting a None
        value in the observation queue.

    :param mkid: [TODO:description]
    :param pv: [TODO:description]
    :param pconf: [TODO:description]
    :param econf: [TODO:description]
    :param q: [TODO:description]
    :param r: [TODO:description]
    """
    logger.info("Manager is awaiting new observations")
    jobs_active = list()
    while True:
        job = q.get()
        if job is None:
            # wait until all workers have terminated
            for worker in jobs_active:
                worker.join()

            break

        graph_id, graph = job

        # listen for new observations in parallel
        thread_id = f"worker-{len(jobs_active)+1}"
        thread = threading.Thread(target=process_graph, name=thread_id,
                                  args=(rng, mkid, pv, graph, graph_id,
                                        pconf, econf, r))
        thread.start()
        jobs_active.append(thread)

        # remove terminated jobs from tracker
        jobs_active = [job for job in jobs_active if job.is_alive()]


def listener(connector: Connector, q: Queue, r: Queue) -> None:
    """ Listen on an endpoint for new messages. Queue
        these upon arrival. This operation is thread safe.

    :param connector: [TODO:description]
    :param q: [TODO:description]
    """
    thread_id = threading.current_thread().name
    for graph_id, graph in connector.listen():
        q.put((graph_id, graph))

    # let the main thread know the worker is terminating
    r.put((thread_id, None))


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

    # use a lock for operations on the pattern vault
    lock = RLock()

    # initiate pattern vault which will manage and track patterns over time
    pv = PatternVault(lock=lock)
    if flags.backup_restore is not None:
        pv = BackupManager.restore_backup(Path(flags.backup_restore))
        pv._lock = lock

        logger.info("Backup restored successfully")

    # setup backup manager to periodically write the pattern vault to disk
    bckmgr = BackupManager(pv, Path(flags.backup_path),
                           lock, flags.backup_interval)
    bckmgr.enable_auto_backup()

    # use queues to communicate between threads
    q = Queue()  # queue observation here
    r = Queue()  # queue reports here

    # listen to all endpoints in parallel
    listening_jobs = list()
    for i, connector in enumerate(adaptor.connectors, 1):
        thread_id = f"listner-{i}"
        thread = threading.Thread(target=listener, name=thread_id,
                                  args=(connector, q, r))
        thread.start()
        listening_jobs.append(thread)

    # start a manager which spawns new threads as new observations arrive
    manager = threading.Thread(target=process_observation, name="manager",
                               args=(rng, mkid, pv, pconf, econf, q, r))
    manager.start()

    # loop until all connections have been terminated
    while len(listening_jobs) > 0:
        try:
            # wait until a new report comes in
            thread_id, report = r.get()
            if report is None:
                logger.info(f"Listner {thread_id} has terminated")

                # terminate and remove listner from tracker
                listening_jobs_new = list()
                for listner in listening_jobs:
                    if listner.name == thread_id:
                        listner.join()

                        continue

                    listening_jobs_new.append(listner)

                listening_jobs = listening_jobs_new

                continue

            # publish report if requested by the report level
            assert isinstance(report, ValidationReport)
            if report.status_code >= econf.report_level:
                if not publish_validation_report(adaptor, report, mkid):
                    logger.info("Unable to publish validation report")
        except Exception as e:
            logger.error(f"Job execution raised execption: {e}")

    # tell workers to terminate
    logger.info("Manager telling workers to terminate")
    q.put(None)

    # wait until manager is terminated
    manager.join()
    logger.info("Manager has been terminated")

    # starting emergency backup
    bckmgr.disable_auto_backup()
    if len(pv) > 0:
        bckmgr.create_backup()  # emergency backup

    logger.info("Waiting on connections to close...")


def __main__():
    adaptor_dir = os.environ.get(ADAPTER_ENV_NAME)
    adaptor_lst = [ROOT_PATH / "adaptors"]
    if adaptor_dir is not None:
        adaptor_dir = Path(adaptor_dir)
        if not adaptor_dir.exists():
            logger.warning(f"Unable to find path '{adaptor_dir}'")
        else:
            adaptor_lst.append(adaptor_dir)

    # find available adaptors
    adaptors = list_classes(adaptor_lst)
    del adaptors['adaptor']  # exclude the abstract base class

    parser = argparse.ArgumentParser(
        prog="GLADoSS",
        description="Graph-based Live Anomaly Detection on Semantic Streams",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        epilog="The development of this program has been funded by HEDGE-IoT")
    parser.add_argument("--backup-interval", help="Intervals between backups. "
                        + "Expects the input to be an integer followed by 'H'"
                        + ", 'D', or 'W', denoting hours, days, or weeks.",
                        type=timeSpanArg, default=None)
    parser.add_argument("--backup-path", help="Directory to write backups to",
                        type=str, default=str(Path().resolve() / "backup"))
    parser.add_argument("--backup-restore", help="Backup file from which to "
                        "import patterns on start.", type=str,
                        default=None)
    parser.add_argument("--seed", help="Seed for random number generator "
                        + "(optional)", type=int, default=None)
    parser.add_argument("--verbose", "-v", help="Show debug messages in "
                        + "console.", action='count', default=0)

    parser_comm = parser.add_argument_group('Communication Settings')
    parser_comm.add_argument("adaptor", help="Adaptor appropriate for "
                             + f"endpoint. Set '{ADAPTER_ENV_NAME}' to "
                             + "support dynamic loading of bespoke adaptors.",
                             choices=list(adaptors.keys()),
                             type=str, nargs='?')
    parser_comm.add_argument("--endpoint", help="HTTP address to listen to. "
                             "This is only needed if the application listens "
                             "to exactly one endpoint and none is provided "
                             "in a separate configuration file.",
                             default="http://127.0.0.1:8000", type=str)
    parser_comm.add_argument("--continuous", help="Keep listening for changes"
                             " in the response, irrespective of response "
                             "status", default=False, action="store_true")
    parser_comm.add_argument("--retries", help="Number of retries on error",
                             default=3, type=int)
    parser_comm.add_argument("--retry-delay", help="Number of seconds to wait "
                             + "before retrying after the occurrence of an "
                             + "error", default=30, type=int)
    parser_comm.add_argument("--return-receipt", help="Send acknowledgement "
                             + "to sender upon reception of message.",
                             action='store_true', default=False)
    parser_comm.add_argument("--request-delay", help="Number of seconds to "
                             + "wait between polling the server.", default=0.1,
                             type=int)

    parser_patt = parser.add_argument_group('Pattern Recognition Settings')
    parser_patt.add_argument("--pattern-decay", help="Number of epoch passed "
                             "until an absent pattern component is forgotten. "
                             "A negative value disables this feature "
                             "entirely.", type=int, default=-1)
    parser_patt.add_argument("--pattern-threshold", help="Number of epoch "
                             "passed until an new pattern component is "
                             "added to the pattern. A negative value disables"
                             " this feature entirely.", type=int, default=-1)
    parser_patt.add_argument("--pattern-resolution", help="Number of "
                             "significant figures to take into account when "
                             "evaluating a new sample. A negative value "
                             "disables this feature.", type=int, default=-1)

    parser_eval = parser.add_argument_group('Anomaly Detection Settings')
    parser_eval.add_argument("--significance-level-critical",
                             help="Significance level (alpha) for the test "
                             "statistic. A p-value less than this level will "
                             "trigger a critical warning.", type=float,
                             default=0.05, dest='alpha_critical')
    parser_eval.add_argument("--significance-level-suspicious",
                             help="Significance level (alpha) for the test "
                             "statistic. A p-value less than this level will "
                             "trigger a warning.", type=float, default=0.10,
                             dest='alpha_suspicious')
    parser_eval.add_argument("--evaluate-structure", help="Evaluate the "
                             "structure of the observed state graph against "
                             "the associated graph pattern.", type=bool,
                             action=argparse.BooleanOptionalAction,
                             default=True)
    parser_eval.add_argument("--evaluate-data", help="Evaluate the "
                             "data of the observed state graph against "
                             "the associated graph pattern.", type=bool,
                             action=argparse.BooleanOptionalAction,
                             default=True)
    parser_eval.add_argument("--evaluate-timestamps", help="Evaluate any "
                             "timestamps of the observed state graph against "
                             "the associated graph pattern.", type=bool,
                             action=argparse.BooleanOptionalAction,
                             default=False)
    parser_eval.add_argument("--grace-period", help="Number of updates to "
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
    parser_eval.add_argument("--match-cwa", help="If enabled, employ the "
                             "Closed World Assumption during the evaluation "
                             "of an observed state graph: expected yet "
                             "missing triples will now trigger a warning.",
                             action='store_true', default=False)
    parser_eval.add_argument("--match-exact", help="If enabled, any missing "
                             "or extra triples in the observed state graph "
                             "will trigger a warning.",
                             action='store_true', default=False)
    parser_eval.add_argument("--report-level", help="Reports of equal level "
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
    assert flags.adaptor is not None, "No adaptor specified - choose one " \
                                      f"from {list(adaptors.keys())}"
    adaptor = import_class(adaptors, flags.adaptor)

    # start main loop
    main(rng, adaptor, flags, cconf, pconf, econf)


if __name__ == "__main__":
    __main__()

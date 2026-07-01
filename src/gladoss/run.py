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
from typing import Callable, Optional

import numpy as np

from gladoss.adaptors.adaptor import Adaptor
from gladoss.core.stores import MemoryStore, PatternVault
from gladoss.data.backup import BackupManager
from gladoss.data.utils import create_namespace_subset, timeSpanArg
from gladoss.core.connector import Connector
from gladoss.core.report import ReportScheduler, ValidationReport
from gladoss.core.utils import gen_id, import_class, init_rng, list_classes
from gladoss.modules.graph.monitor import process_graph
from gladoss.modules.stream.monitor import process_stream


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
                              label: Optional[int | list[int]],
                              namespace: Optional[str],
                              mkid: Callable) -> bool:
    """ Convert the validation report to RDF graph format and publish
        the result via the adaptor.

    :param adaptor: [TODO:description]
    :param report: [TODO:description]
    :param mkid: [TODO:description]
    :return: [TODO:description]
    """
    # represent validation report
    logger.debug(f"Preparing publication of validation report "
                 f"({report.subject_id})")
    report_graph = report.to_graph(namespace, mkid)
    logger.debug(f" {{\n{'\n  '.join([str(s) for s in report_graph])}\n  }}")

    # publish report to endpoint
    logger.info(f"Publishing validation report ({report.subject_id})")
    success = adaptor.publish_report(report.type, report.subject_id,
                                     report_graph, label)

    return success


def process_message(rng: np.random.Generator, mkid: Callable,
                    sc_store: MemoryStore, gp_store: PatternVault,
                    pconf: SimpleNamespace, econf: SimpleNamespace,
                    q_obs: Queue, q_rpt: Queue, q_rpts: Queue) -> None:
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
    logger.info("Manager is awaiting new messages")
    jobs_active = list()
    while True:
        job = q_obs.get()
        if job is None:
            # wait until all workers have terminated
            for worker in jobs_active:
                worker.join()

            break

        (node_id, graph_id, graph_data, graph_label), endpoint, rtime = job
        logger.info(f"Received new message from '{endpoint}'")

        # process stream info in parallel
        thread_id = f"worker-{len(jobs_active)+1}"
        thread = threading.Thread(target=process_stream, name=thread_id,
                                  args=(sc_store, node_id, endpoint, rtime,
                                        econf, q_rpt, q_rpts))
        thread.start()
        jobs_active.append(thread)

        # process new graphs in parallel
        thread_id = f"worker-{len(jobs_active)+1}"
        thread = threading.Thread(target=process_graph, name=thread_id,
                                  args=(rng, mkid, gp_store, graph_data,
                                        graph_id, graph_label, rtime,
                                        pconf, econf, q_rpt))
        thread.start()
        jobs_active.append(thread)

        # remove terminated jobs from tracker
        jobs_active = [job for job in jobs_active if job.is_alive()]


def listener(connector: Connector, q_obs: Queue, q_rpt: Queue)\
        -> None:
    """ Listen on an endpoint for new messages. Queue
        these upon arrival. This operation is thread safe.

    :param connector: [TODO:description]
    :param q: [TODO:description]
    """
    thread_id = threading.current_thread().name
    for package, endpoint in connector.listen():
        rtime = datetime.now()  # reception time

        # new observation
        q_obs.put((package, endpoint, rtime))

    # let the main thread know the worker is terminating
    q_rpt.put((thread_id, (None, None)))


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
    for i in range(cconf.retries + 1):
        try:
            adaptor = adaptor_cls(controller=controller,  # type: ignore
                                  config=cconf)
        except Exception as e:
            logger.error(e)
            if i < cconf.retries or cconf.continuous:
                logger.info("Adaptor initialization failed. Retrying...")
                if controller.wait(cconf.retry_delay):
                    # termination signal received during wait
                    break

                continue

            raise

        break

    # initiate store to track stream characteristics; lock for multithreading
    sc_store = MemoryStore(sc_lock := RLock(), pconf.pattern_decay)

    # initiate pattern vault which will manage and track patterns over time
    gp_store = PatternVault(gp_lock := RLock())

    # restore saved states
    if flags.backup_restore is not None:
        bck_stores = BackupManager.restore_backup(Path(flags.backup_restore))
        for store_name, store_obj in bck_stores:
            if store_name == "sc_store":
                sc_store = store_obj
                sc_store._lock = sc_lock

                break
            if store_name == "gp_store":
                gp_store = store_obj
                gp_store._lock = gp_lock

                break

        logger.info("Backup restored")

    # setup backup manager to periodically write the pattern vault to disk
    bckmgr = BackupManager(location=Path(flags.backup_path),
                           stores=[("gp_store", gp_store),
                                   ("sc_store", sc_store)],
                           interval=flags.backup_interval)
    bckmgr.enable_auto_backup()

    # use queues to communicate between threads
    q_obs = Queue()  # queue observation here
    q_rpt = Queue()  # queue reports here
    q_rpts = Queue()  # queue scheduled reports here

    # listen to all endpoints in parallel
    listening_jobs = list()
    for i, connector in enumerate(adaptor.connectors, 1):
        thread_id = f"listner-{i}"
        thread = threading.Thread(target=listener, name=thread_id,
                                  args=(connector, q_obs, q_rpt))
        thread.start()
        listening_jobs.append(thread)

    # start a manager which spawns new threads as new observations arrive
    manager = threading.Thread(target=process_message, name="manager",
                               args=(rng, mkid, sc_store, gp_store,
                                     pconf, econf, q_obs, q_rpt, q_rpts))
    manager.start()

    # start report scheduler if requested
    report_sheduler = ReportScheduler(q_rpts, q_rpt)
    if econf.proactive_notification:
        report_sheduler.enable()

    # loop until all connections have been terminated
    while len(listening_jobs) > 0:
        try:
            # wait until a new report comes in
            thread_id, (report, label) = q_rpt.get()
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
            logger.debug("Processing validation report with status "
                         f"{report.status_code.name} "
                         f"({report.subject_id})")
            if report.status_code >= econf.report_level:
                if not publish_validation_report(adaptor, report, label,
                                                 flags.namespace, mkid):
                    logger.info("Unable to publish validation report "
                                f"({report.subject_id})")
        except Exception as e:
            logger.error(f"Job execution raised execption: {e}")

    # tell workers to terminate
    logger.info("Manager telling workers to terminate")
    q_obs.put(None)

    # wait until manager is terminated
    manager.join()
    logger.info("Manager has been terminated")

    # tell scheduler to terminate
    report_sheduler.disable()

    # starting emergency backup
    bckmgr.disable_auto_backup()
    if len(gp_store) > 0:  # sc_store is tied to gp_store
        bckmgr.create_backup()  # emergency backup

    logger.info("Waiting on connections to close...")


def __main__():
    adaptor_dir = os.environ.get(ADAPTER_ENV_NAME)
    adaptor_lst = [ROOT_PATH / "adaptors" / "default"]
    if adaptor_dir is not None:
        adaptor_dir = Path(adaptor_dir)
        if not adaptor_dir.exists():
            logger.warning(f"Unable to find path '{adaptor_dir}'")
        else:
            adaptor_lst.append(adaptor_dir)

    # find available adaptors
    adaptors = list_classes(adaptor_lst)

    parser = argparse.ArgumentParser(
        prog="GLADoSS",
        description="Graph-based Live Anomaly Detection on Semantic Streams",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        epilog="The development of this program has been funded by HEDGE-IoT")
    parser.add_argument("--backup-interval", help="Intervals between backups. "
                        + "Expects the input to be an integer followed by 'M' "
                        + "'H', 'D', or 'W', denoting minutes, hours, days, "
                        + "or weeks.", type=timeSpanArg, default=None)
    parser.add_argument("--backup-path", help="Directory to write backups to",
                        type=str, default=str(Path().resolve() / "backup"))
    parser.add_argument("--backup-restore", help="Backup file from which to "
                        "import patterns on start.", type=str,
                        default=None)
    parser.add_argument("--namespace", help="Namespace to use when generating "
                        "new identifiers. Omit for blank nodes (default).",
                        type=str, default=None)
    parser.add_argument("--seed", help="Seed for random number generator "
                        + "(optional)", type=int, default=None)
    parser.add_argument("--verbose", "-v", help="Show debug messages in "
                        + "console.", action='count', default=0)

    parser_comm = parser.add_argument_group('Communication Settings')
    parser_comm.add_argument("adaptor", help="Adaptor appropriate for "
                             + f"endpoint. Set '{ADAPTER_ENV_NAME}' to "
                             + "support dynamic loading of bespoke adaptors.",
                             choices=list(adaptors.keys()),
                             type=str, nargs='?', default='restful')
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
                             default=0.01, dest='alpha_critical')
    parser_eval.add_argument("--significance-level-suspicious",
                             help="Significance level (alpha) for the test "
                             "statistic. A p-value less than this level will "
                             "trigger a warning.", type=float, default=0.05,
                             dest='alpha_suspicious')
    parser_eval.add_argument("--evaluate-structure", help="Evaluate the "
                             "structure of the observed state graph against "
                             "the associated graph pattern.",
                             action=argparse.BooleanOptionalAction,
                             default=True)
    parser_eval.add_argument("--evaluate-data", help="Evaluate the "
                             "data of the observed state graph against "
                             "the associated graph pattern.",
                             action=argparse.BooleanOptionalAction,
                             default=True)
    parser_eval.add_argument("--evaluate-stream", help="Evaluate transmission "
                             "characteristics associated with the observed "
                             "state graph against historical data points.",
                             action=argparse.BooleanOptionalAction,
                             default=False)
    parser_eval.add_argument("--proactive-notification", help="Report on "
                             "streams from which awaited messages have not "
                             "been received within the expected interval.",
                             action=argparse.BooleanOptionalAction,
                             default=False)
    parser_eval.add_argument("--evaluate-timestamps", help="Evaluate any "
                             "timestamps of the observed state graph against "
                             "the associated graph pattern.",
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
    parser_eval.add_argument("--pi-right-sided", help="Only report on "
                             "violations on the right-hand side of the "
                             "prediction interval. This corresponds with "
                             "values that exceed the maximum allowed value.",
                             action='store_true', default=False)
    parser_eval.add_argument("--pi-tolerance", help="Relative tolerance in "
                             "[0, 1] applied to the prediction interval. A "
                             "higher value results in a more constrained "
                             "interval.", default=0.1, type=float,
                             dest='pi_tolerance')
    parser_eval.add_argument("--report-level", help="Reports of equal level "
                             "and higher will be published to the endpoint: "
                             "NOMINAL behaviour (0), generic ERRORS (1), "
                             "INSUFFICIENT DATA (2), INCONSISTENCIES (3), "
                             "SUSPICIOUS warnings (4), and CRITICAL "
                             "warnings (5)",
                             type=int, default=3)

    flags = parser.parse_args()

    # parameter sanity check
    assert 0 <= flags.retries
    assert 0 <= flags.retry_delay
    assert 0. <= flags.request_delay

    assert -1 <= flags.pattern_decay
    assert -1 <= flags.pattern_threshold
    assert -1 <= flags.pattern_resolution

    assert 0. <= flags.alpha_critical <= 1.
    assert 0. <= flags.alpha_suspicious <= 1.
    assert 0 < flags.grace_period
    assert 0 < flags.samplesize
    assert 0 <= flags.samplegap
    assert 0. <= flags.pi_tolerance <= 1.
    assert 0 <= flags.report_level <= 5

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
                                            'evaluate_stream',
                                            'evaluate_timestamps',
                                            'grace_period',
                                            'samplesize',
                                            'samplegap',
                                            'match_cwa',
                                            'match_exact',
                                            'pi_right_sided',
                                            'pi_tolerance',
                                            'proactive_notification',
                                            'report_level'])

    # set log level
    log_level = logging.NOTSET
    log_format = "[%(asctime)s] - %(message)s"
    if flags.verbose >= 2:
        log_level = logging.DEBUG
        log_format = ("[%(asctime)s] [%(levelname)s] [%(threadName)s] "
                      "%(module)s / %(funcName)s - %(message)s")
    elif flags.verbose == 1:
        log_level = logging.INFO
        log_format = "[%(asctime)s] [%(levelname)s] %(module)s - %(message)s"
    else:
        log_level = logging.WARNING

    logging.basicConfig(level=log_level,
                        format=log_format)

    logger.debug("\n".join([f"{k}: {v}" for k, v in cconf.__dict__.items()]))
    logger.debug("\n".join([f"{k}: {v}" for k, v in pconf.__dict__.items()]))
    logger.debug("\n".join([f"{k}: {v}" for k, v in econf.__dict__.items()]))

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

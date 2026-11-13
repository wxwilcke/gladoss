#!/usr/bin/env python

import argparse
import hashlib
import logging
import os
import signal
import threading
from datetime import datetime
from pathlib import Path
from queue import Queue
from threading import Event
from types import SimpleNamespace

import numpy as np

from gladoss.adaptors.adaptor import Adaptor
from gladoss.core.connector import Connector
from gladoss.core.report import ReportScheduler, ValidationReport
from gladoss.core.utils import import_class, init_rng, list_classes
from gladoss.data.backup import BackupManager
from gladoss.data.utils import create_namespace_subset, pathArg, timeSpanArg
from gladoss.modules.graph.monitor import GraphProcessor
from gladoss.modules.graph.report import GraphValidationReport
from gladoss.modules.node.monitor import NodeProcessor
from gladoss.modules.processor import MessageProcessor
from gladoss.modules.stream.monitor import StreamProcessor

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
                              label: int | list[int] | None,
                              namespace: str | None) -> bool:
    """ Convert the validation report to RDF graph format and publish
        the result via the adaptor.

    :param adaptor: [TODO:description]
    :param report: [TODO:description]
    :param mkid: [TODO:description]
    :return: [TODO:description]
    """
    # represent validation report
    logger.debug(f"Preparing publication of validation report "
                 f"(SID '{report.stream_id}')")
    report_graph = report.to_graph(namespace)

    graph_id_str = ("\n graph:\t\t'" + report.graph_id + "'"
                    if report.graph_id is not None else '')
    graph_str = "\n  {}".format(
            "\n  ".join([str(s) for s in report_graph]))
    logger.debug(f"""OUTGOING TRANSMISSION
BEGIN
 timestamp:\t{report.timestamp.isoformat()}
 node:\t\t'{report.node_id}'
 stream:\t'{report.stream_id}'{graph_id_str}
 data: {graph_str}
END
    """)

    # publish report to endpoint
    logger.info(f"Publishing validation report (SID '{report.stream_id}')")
    success = adaptor.publish_report(report.type, report.stream_id,
                                     report_graph, label)

    return success


def _start_predictive_maintenance_worker(prmn_proc: NodeProcessor,
                                         report: GraphValidationReport,
                                         workers: list[threading.Thread])\
        -> list[threading.Thread]:
    """ Process produced validation reports in a new thread that
        assesses and updates node health status. This is only used
        by the predictive maintenance module. Returns an updated list
        of active threads.

    :param prmn_proc: [TODO:description]
    :param report: [TODO:description]
    :param workers: [TODO:description]
    :return: [TODO:description]
    """
    thread_id = f"worker_pm-{len(workers)+1}"
    thread = threading.Thread(target=prmn_proc.process,
                              name=thread_id,
                              args=(report,))
    thread.start()

    workers.append(thread)
    workers = [worker for worker in workers if worker.is_alive()]

    return workers


def process_message(q_msg: Queue, msg_proc_lst: list[MessageProcessor])\
        -> None:
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
    workers_active = []
    while True:
        msg = q_msg.get()
        if msg is None:
            # wait until all workers have terminated
            for worker in workers_active:
                worker.join()

            break

        (node_id, stream_id, graph_data, graph_label), endpoint, rtime = msg
        # TODO: each stream is currently associated with exactly one graph. In
        # reality, one stream can produce multiple graphs. Also, a single graph
        # can diverge into two or more patterns (e.g., weekdays and weekends)
        # which would necessitate different identifiers (instead of a single
        # stream ID).
        graph_id = hashlib.md5(stream_id.encode("UTF-8")).hexdigest()
        logger.info(f"Received new message from node '{node_id}' via "
                    f"endpoint '{endpoint}' in stream '{stream_id}'.")

        graph_str = "\n  {}".format(
                "\n  ".join([str(s) for s in graph_data]))
        logger.debug(f"""INCOMING TRANSMISSION
BEGIN
 timestamp:\t{rtime.isoformat()}
 endpoint:\t'{endpoint}'
 node:\t\t'{node_id}'
 stream:\t'{stream_id}'
 graph:\t\t'{graph_id}'
 data: {graph_str}
END
        """)

        # push data to appropriate message processor in new thread
        for proc in msg_proc_lst:
            thread_id = f"worker-{len(workers_active)+1}"
            thread = threading.Thread(target=proc.process, name=thread_id,
                                      args=(endpoint, node_id, stream_id,
                                            graph_id, graph_data, graph_label,
                                            rtime))
            thread.start()
            workers_active.append(thread)

        # remove terminated workers from tracker
        workers_active = [worker for worker in workers_active
                          if worker.is_alive()]


def listener(connector: Connector, q_msg: Queue, q_rpt: Queue)\
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
        q_msg.put((package, endpoint, rtime))

    # let the main thread know the worker is terminating
    q_rpt.put((thread_id, (None, None)))


def _init_adaptor(adaptor_cls: type[Adaptor], conn_cfg: SimpleNamespace)\
        -> Adaptor | None:
    adaptor = None
    for i in range(conn_cfg.retries + 1):
        try:
            adaptor = adaptor_cls(controller=controller,  # type: ignore
                                  config=conn_cfg)
        except Exception as e:
            logger.error(e)
            if i < conn_cfg.retries or conn_cfg.continuous:
                logger.info("Adaptor initialization failed. Retrying...")
                if controller.wait(conn_cfg.retry_delay):
                    # termination signal received during wait
                    break

                continue

            raise

        break

    return adaptor


def _spawn_connector_workers(adaptor: Adaptor, q_msg: Queue, q_rpt: Queue)\
        -> list[threading.Thread]:
    listening_jobs = []
    for i, connector in enumerate(adaptor.connectors, 1):
        thread_id = f"listner-{i}"
        thread = threading.Thread(target=listener, name=thread_id,
                                  args=(connector, q_msg, q_rpt))
        thread.start()
        listening_jobs.append(thread)

    return listening_jobs


def _rmv_connector_workers(jobs: list[threading.Thread], connector_id: str)\
        -> list[threading.Thread]:
    jobs_new = []
    for listner in jobs:
        if listner.name == connector_id:
            listner.join()

            continue

        jobs_new.append(listner)

    return jobs_new


def _restore_backup(strm_proc: StreamProcessor,
                    grph_proc: GraphProcessor,
                    flags: argparse.Namespace):
    bck_stores = BackupManager.restore_backup(flags.backup_restore)
    for store_name, store_obj in bck_stores:
        if store_name == "strm_store":
            strm_lock = strm_proc.store._lock

            strm_proc.store = store_obj
            strm_proc.store._lock = strm_lock

            continue
        if store_name == "grph_store":
            grph_lock = grph_proc.store._lock

            grph_proc.store = store_obj
            grph_proc.store._lock = grph_lock

            continue


def main(rng: np.random.Generator, adaptor_cls: type[Adaptor],
         flags: argparse.Namespace, conn_cfg: SimpleNamespace,
         grph_cfg: SimpleNamespace, strm_cfg: SimpleNamespace,
         prmn_cfg: SimpleNamespace) -> None:
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

    # setup adaptor to manage communication and to translate incoming messages
    adaptor = _init_adaptor(adaptor_cls, conn_cfg)
    if adaptor is None:
        return

    # use queues to communicate between threads
    q_msg = Queue()  # queue incoming messages here
    q_rpt = Queue()  # queue outgoing reports here
    q_rpts = Queue()  # queue scheduled reports here

    # initiate message processors
    strm_proc = StreamProcessor(q_out=q_rpt, q_shed=q_rpts,
                                rng=rng, cfg=strm_cfg)
    grph_proc = GraphProcessor(q_out=q_rpt, q_shed=q_rpts,
                               rng=rng, cfg=grph_cfg)

    prmn_proc = None
    if flags.predictive_maintenance:
        prmn_proc = NodeProcessor(q_out=q_rpt, q_shed=Queue(),
                                  rng=rng, cfg=prmn_cfg)

    # restore saved states
    if flags.backup_restore is not None:
        _restore_backup(strm_proc, grph_proc, flags)
        logger.info("Backup restored")

    # setup backup manager to periodically write the stores to disk
    bckmgr = BackupManager(location=flags.backup_path,
                           stores=[("grph_store", grph_proc.store),
                                   ("strm_store", strm_proc.store)],
                           interval=flags.backup_interval)
    bckmgr.enable_auto_backup()

    # listen to all endpoints in parallel
    listening_jobs = _spawn_connector_workers(adaptor, q_msg, q_rpt)

    # start a manager which spawns new threads as new observations arrive
    manager = threading.Thread(target=process_message, name="manager",
                               args=(q_msg, [grph_proc, strm_proc]))
    manager.start()

    # start report scheduler if requested
    report_sheduler = ReportScheduler(q_rpts, q_rpt)
    if flags.proactive_notification:
        report_sheduler.enable()

    # keep track of predictive maintenance threads
    pm_workers = []

    # loop until all connections have been terminated
    while len(listening_jobs) > 0:
        try:
            # wait until a new report comes in
            thread_id, (report, label) = q_rpt.get()
            if report is None:
                logger.info(f"Listner {thread_id} has terminated")

                # terminate and remove listner from tracker
                listening_jobs = _rmv_connector_workers(listening_jobs,
                                                        thread_id)

                continue

            # publish report if requested by the report level
            assert isinstance(report, ValidationReport)
            logger.debug("Processing validation report with status "
                         f"{report.status_code.name} "
                         f"(SID '{report.stream_id}')")
            if (report.status_code >= flags.report_level and
                not publish_validation_report(adaptor, report, label,
                                              flags.namespace)):
                logger.info("Unable to publish validation report "
                            f"(SID '{report.stream_id}')")

            if flags.predictive_maintenance\
                    and isinstance(report, GraphValidationReport):
                # spawn new thread to evaluate node health and update list
                pm_workers = _start_predictive_maintenance_worker(prmn_proc,
                                                                  report,
                                                                  pm_workers)
        except Exception as e:
            logger.error(f"Job execution raised execption: {e}")

    # tell workers to terminate
    logger.info("Telling workers to terminate")
    q_msg.put(None)
    for pm_worker in pm_workers:
        # wait for predictive maintenance threads to terminate
        pm_worker.join()

    # wait until manager is terminated
    manager.join()
    logger.info("Manager has been terminated")

    # tell scheduler to terminate
    report_sheduler.disable()

    # # starting emergency backup
    bckmgr.disable_auto_backup()
    if len(grph_proc.store) > 0:  # strm_store is tied to grph_store
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
    parser.add_argument("--backup-path", help="Directory to write backups to, "
                        "either absolute or relative to the working "
                        "directory.", type=pathArg,
                        default=Path().cwd() / "backup")
    parser.add_argument("--backup-restore", help="Path to backup file (.bak), "
                        "either absolute or relative to backup path, from "
                        "which to import patterns on start.", type=pathArg,
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
    parser_eval.add_argument("--noise-ratio", help="Add Gaussian noise to "
                             "the population before evaluation, scaled by "
                             "the square root of the population's standard "
                             "deviation. A value of zero disables this "
                             "feature (default).", type=float, default=0.)
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
                             "NON-CRITICAL warnings (4), and CRITICAL "
                             "warnings (5)",
                             type=int, default=3)

    parser_pred = parser.add_argument_group('Predictive Maintenance Settings')
    parser_pred.add_argument("--predictive-maintenance",
                             help=("If enabled, monitor node health status "
                                   "over time and report when health "
                                   "degration exceeds set rate limits."),
                             action=argparse.BooleanOptionalAction,
                             default=False)
    parser_pred.add_argument("--weight-decay-rate",
                             help=("The value used to decay the weight of an "
                                   "undesired state with over time. Higher "
                                   "values translate to a stronger decay "
                                   "rate."),
                             type=float, default=0.2)
    parser_pred.add_argument("--weight-decay-interval",
                             help=("The interval in seconds between decay "
                                   "events of an undesired state associated "
                                   "with a node. Higher values translate to "
                                   "faster decay."),
                             type=float, default=60.0)
    parser_pred.add_argument("--weight-multiplier-noncritical",
                             help=("The value to multiply the weight of an "
                                   "undesired state with on observing a "
                                   "non-critical validation failure. "
                                   "Higher values translate to a stronger "
                                   "increase of the undesired state."),
                             type=float, default=1.2)
    parser_pred.add_argument("--weight-multiplier-critical",
                             help=("The value to multiply the weight of an "
                                   "undesired state with on observing a "
                                   "critical validation failure. "
                                   "Higher values translate to a stronger "
                                   "increase of the undesired state."),
                             type=float, default=1.5)
    parser_pred.add_argument("--weight-growth-rate-noncritical",
                             help=("The growth rate of the weight of an "
                                   "undesired state since the last "
                                   "observation that will trigger the "
                                   "publication of a non-critical "
                                   "maintenance report. Higher values "
                                   "translate to more liberal behaviour."),
                             type=float, default=1.5)
    parser_pred.add_argument("--weight-growth-rate-critical",
                             help=("The growth rate of the weight of an "
                                   "undesired state since the last "
                                   "observation that will trigger the "
                                   "publication of a non-critical "
                                   "maintenance report. Higher values "
                                   "translate to more liberal behaviour."),
                             type=float, default=2.0)

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
    assert 0. <= flags.noise_ratio <= 1.
    assert 0 < flags.samplesize
    assert 0 <= flags.samplegap
    assert 0. <= flags.pi_tolerance <= 1.
    assert 0 <= flags.report_level <= 5

    assert 0. <= flags.weight_decay_rate <= 1.0
    assert 1.0 <= flags.weight_multiplier_noncritical
    assert 1.0 <= flags.weight_multiplier_critical
    assert 1.0 <= flags.weight_growth_rate_noncritical
    assert 1.0 <= flags.weight_growth_rate_critical

    # flags for connectors
    conn_cfg = create_namespace_subset(flags,
                                       ['adaptor',
                                        'endpoint',
                                        'continuous',
                                        'retries',
                                        'retry_delay',
                                        'request_delay',
                                        'return_receipt'])
    # flags for stream processor
    strm_cfg = create_namespace_subset(flags,
                                       ['pattern_decay',
                                        'evaluate_stream',
                                        'proactive_notification',
                                        'grace_period',
                                        'alpha_critical',
                                        'alpha_suspicious',
                                        'pi_right_sided',
                                        'pi_tolerance'])

    # flags for graph processor
    grph_cfg = create_namespace_subset(flags,
                                       ['pattern_decay',
                                        'pattern_threshold',
                                        'pattern_resolution',
                                        'grace_period',
                                        'match_cwa',
                                        'match_exact',
                                        'alpha_critical',
                                        'alpha_suspicious',
                                        'evaluate_timestamps',
                                        'samplesize',
                                        'samplegap',
                                        'pi_tolerance',
                                        'noise_ratio',
                                        'evaluate_structure',
                                        'evaluate_data'])

    # flags for predictive maintenance
    prmn_cfg = create_namespace_subset(flags,
                                       ['weight_decay_rate',
                                        'weight_decay_interval',
                                        'weight_multiplier_noncritical',
                                        'weight_multiplier_critical',
                                        'weight_growth_rate_noncritical',
                                        'weight_growth_rate_critical'])

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

    logger.debug(
        "Using following configuration options\n - "
        + "\n - ".join([f"{k}: {v}" for k, v in flags._get_kwargs()]))

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

    # Set backup restore path to be relative to backup path
    if flags.backup_restore is not None \
       and not flags.backup_restore.is_absolute():
        # make relative path absolute
        flags.backup_restore = flags.backup_path / flags.backup_restore

    # start main loop
    main(rng, adaptor, flags, conn_cfg, grph_cfg, strm_cfg, prmn_cfg)


if __name__ == "__main__":
    __main__()

#!/usr/bin/env python

import argparse
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
import logging
from queue import Queue
import signal
from threading import Event

from rdf.graph import Statement
from rdf.terms import Resource

from gladoss.adaptors.adaptor import Adaptor
from gladoss.data.backup import BackupManager
from gladoss.data.utils import timeSpanArg
from gladoss.core.monitor import Monitor
from gladoss.core.pattern import GraphPattern, PatternVault, ValidationReport
from gladoss.core.utils import import_module


logger = logging.getLogger(__name__)

_ADAPTORS = {
        "dummy": ["dummy", "DummyAdaptor"],
        "knowledge_engine": ["knowledge_engine", "KE_Adaptor"]
        }


def signal_handler(signum, frame):
    signal.signal(signum, signal.SIG_IGN)
    logger.info("Received Keyboard Interrupt")

    global controller
    controller.set()


def create_validation_report(pattern: GraphPattern, fact_set: set[Statement])\
        -> ValidationReport:
    pass


def publish_validation_report(adaptor: Adaptor, report: ValidationReport):
    pass


def listener(monitor: Monitor, q: Queue) -> None:
    for fact_set, anchor_set in monitor.listen():
        q.put((fact_set, anchor_set))


def process_observation(pv: PatternVault, fact_set: set[Statement],
                        anchor_set: set[Resource]):
    pattern = pv.find_associated_pattern(anchor_set)
    if pattern is None:
        logger.debug(f"Associated pattern not found: {fact_set}")
        pattern = pv.create_associated_pattern(fact_set, anchor_set)

    report = create_validation_report(pattern, fact_set)
    if report is None:  # no suspicious behaviour detected
        pv.update_associated_pattern(pattern, fact_set)

    return report


def main(adaptor_cls: Adaptor, flags: argparse.Namespace):
    logger.info("Initiating Program")

    adaptor = adaptor_cls()
    monitor = Monitor(adaptor=adaptor,
                      endpoint=flags.endpoint,
                      continuous=flags.continuous,
                      num_retries=flags.retries,
                      retry_delay=flags.retry_delay,
                      request_delay=flags.request_delay,
                      controller=controller)

    pv = PatternVault()

    # setup backup manager
    bckmgr = BackupManager(pv, flags.backup_path, flags.backup_interval)
    bckmgr.enable_auto_backup()

    with ThreadPoolExecutor as executor:
        q = Queue()
        listener_fs = executor.submit(listener, monitor, q)

        jobs_active = {listener_fs}
        while len(jobs_active) > 0:
            # check which jobs have been completed
            jobs_completed, _ = wait(jobs_active, return_when=FIRST_COMPLETED)

            while not q.empty():
                # process incoming messages
                # spawn a new thread for each
                job = q.get()

                fact_set, anchor_set = job
                job_fs = executor.submit(process_observation,
                                         pv, fact_set, anchor_set)
                jobs_active.add(job_fs)

            # process output of completed jobs
            for job_fs in jobs_completed:
                report = None
                try:
                    report = job_fs.result()
                except Exception as e:
                    logger.error(f"Job execution raised execption: {e}")

                    continue

                if report is not None:  # suspicious behaviour detected
                    publish_validation_report(adaptor, report)

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
    parser.add_argument("adaptor", help="Adaptor appropriate for endpoint",
                        choices=list(_ADAPTORS.keys()), type=str, nargs=1)
    parser.add_argument("--backup_interval", help="Intervals between backups. "
                        + "Expects the input to be an integer followed by 'H'"
                        + ", 'D', or 'W', denoting hours, days, or weeks.",
                        type=timeSpanArg, default=None)
    parser.add_argument("--backup_path", help="Directory to write backups to",
                        type=str, default="/tmp/")  # FIXME: change
    parser.add_argument("--endpoint", help="HTTP address to listen to",
                        default="http://127.0.0.1:8000", type=str)
    parser.add_argument("--continuous", help="Keep listening for changes in "
                        + "the response, irrespective of response status",
                        default=False, action="store_true")
    parser.add_argument("--retries", help="Number of retries on error",
                        default=3, type=int)
    parser.add_argument("--retry_delay", help="Number of seconds to wait "
                        + "before retrying after the occurrence of an error",
                        default=30, type=int)
    parser.add_argument("--request_delay", help="Number of seconds to wait "
                        + "between polling the server.", default=0.5, type=int)
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

    # register SIGINT signal handler
    global controller
    controller = Event()

    signal.signal(signal.SIGINT, signal_handler)

    # import specified adaptor
    adaptor = import_module(_ADAPTORS, flags.adaptor)

    # start main loop
    main(adaptor, flags)

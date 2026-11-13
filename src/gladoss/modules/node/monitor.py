#!/usr/bin/env python

import logging
import threading
from queue import Queue
from types import SimpleNamespace

import numpy as np

from gladoss.core.report import (
    BECAUSE,
    QED,
    ValidationReport,
)
from gladoss.modules.node.report import MaintenanceReport
from gladoss.modules.node.store import WeightStore
from gladoss.modules.processor import ReportProcessor

logger = logging.getLogger(__name__)


class NodeProcessor(ReportProcessor):
    def __init__(self,
                 q_out: Queue,
                 q_shed: Queue,
                 rng: np.random.Generator,
                 cfg: SimpleNamespace):
        super().__init__(q_out=q_out, q_shed=q_shed, rng=rng, cfg=cfg)

        # initiate store which will manage and track patterns over time
        self.store = WeightStore(threading.RLock())

        self.wd_scheduler = WeightDecayScheduler(self.store, self.q_shed)
        self.wd_scheduler.enable()

    def __del__(self):
        self.wd_scheduler.disable()

    def process(self, report: ValidationReport):
        thread_id = threading.current_thread().name

        node_id = report.node_id
        # register node if unknown
        with self.store._lock:
            if node_id not in self.store.nodes:
                logger.info(f"Registering new node ('{node_id}')")
                if not self.store.register_node(node_id):
                    # unable to register node
                    return

        # schedule decay event
        # NOTE: this uses a preset decay rate and interval, which may be too
        # simplistic when monitoring a heterogeneous set of devices. A better
        # solution would be to let the interval depend on the publication
        # interval of the devices being monitored, and the decay rate on the
        # device type.  A less complex solution might be to allow these values
        # to be set per device (type) using a config file.
        self.q_shed.put((node_id,
                         float(self.cfg.weight_decay_rate),
                         float(self.cfg.weight_decay_interval)))

        # compute weight multiplier based on validation report
        weight_multiplier = computeWeightMultiplier(report, self.cfg)

        # calculate new weight and update node state
        weight_old = weight_new = 1.
        with self.store._lock:
            _, weight_old = self.store.get(node_id)
            weight_new = weight_old * weight_multiplier
            if not self.store.update(node_id, weight_new, report.timestamp):
                # unable to update node state
                return

        # calculate rate of growth
        weight_rog = 1. + (weight_new - weight_old) / weight_old

        # create maintenance report
        status_msg_lst = []
        if weight_rog >= self.cfg.weight_growth_rate_critical:
            status_code = MaintenanceReport.StatusCode.CRITICAL
            status_msg = "Critical Node Health Degration"
            status_msg_long = (
                "Node health rate of degration exceeds critical level "
                f"{BECAUSE} "
                f"OBSERVED: rate = {weight_rog:.2f} "
                f"EXPECTED: rate < {self.cfg.weight_growth_rate_critical} "
                f"{QED}"
                )
            violation_type = ValidationReport.ViolationType.NHS1
        elif weight_rog >= self.cfg.weight_growth_rate_noncritical:
            status_code = MaintenanceReport.StatusCode.NONCRITICAL
            status_msg = "Non-Critical Node Health Degration"
            status_msg_long = (
                "Node health rate of degration exceeds non-critical level "
                f"{BECAUSE} "
                f"OBSERVED: rate = {weight_rog:.2f} "
                f"EXPECTED: rate < {self.cfg.weight_growth_rate_noncritical} "
                f"{QED}"
                )
            violation_type = ValidationReport.ViolationType.NHS2
        else:
            status_code = MaintenanceReport.StatusCode.NOMINAL
            status_msg = "Node Health Nominal"
            status_msg_long = (
                "Node health rate of degration within acceptable levels "
                f"{BECAUSE} "
                f"OBSERVED: rate = {weight_rog:.2f} "
                f"EXPECTED: rate < {self.cfg.weight_growth_rate_noncritical} "
                f"{QED}"
                )
            violation_type = None

        status_msg_lst.append((status_msg, status_msg_long,
                               status_code, violation_type))

        report = MaintenanceReport(node_id=node_id,
                                   stream_id=report.stream_id,
                                   timestamp=report.timestamp,
                                   status_code=status_code,
                                   status_msg_lst=status_msg_lst)

        self.q_out.put((thread_id, (report, None)))


def computeWeightMultiplier(report: ValidationReport,
                            conf: SimpleNamespace) -> float:
    """ Extract the severity labels of all results in the observed validation
        report and use these to compute an appropriate weight multiplier.

        NOTE: the current procedure takes all severity labels in a single
        validation report about a node into account. This might unjustly
        inflate the weight multiplier since different errors in the same
        message can originate from the same underlying problem. Possible
        solutions are 1) including only the highest label, either directly
        (same as if there were just one label) or weighted by the number of
        such labels, or 2) use a function of all labels to compute a multiplier
        that is higher than a single label but less than the current procedure.

    :param graph: [TODO:description]
    :param node_id: [TODO:description]
    :param conf: [TODO:description]
    :return: [TODO:description]
    """
    # calculate weight multiplier
    weight_multiplier = 1.
    if report.status_code == ValidationReport.StatusCode.CRITICAL:
        weight_multiplier *= conf.weight_muliplier_critical
    elif report.status_code == ValidationReport.StatusCode.NONCRITICAL:
        weight_multiplier *= conf.weight_muliplier_noncritical

    return weight_multiplier


class WeightDecayScheduler:
    def __init__(self, store: WeightStore, q_scheduler: Queue):
        self.store = store
        self.q_scheduler = q_scheduler

        self.timers_active = {}  # type: dict[str, threading.Timer]
        self.enabled = False

    def _schedule(self):
        """ Process new scheduling jobs as they come in, by starting a
            timer that will decay the weight associated with a node state
            when the time triggers.
        """
        while True:
            job = self.q_scheduler.get()
            if job is None:
                break

            node_id, decay_rate, decay_interval = job

            # clean up completed timers
            self.timers_active = {
                node_id: timer for node_id, timer in self.timers_active.items()
                if timer.is_alive()}

            # skip scheduling timer if already registered
            if node_id in self.timers_active:
                continue

            if decay_rate is None or decay_interval is None:
                # this was a cancel request
                logger.debug("Cancelled scheduled weight decay "
                             f"({node_id})")

                continue

            assert type(decay_rate) is float
            assert type(decay_interval) is float

            # create and start new timer
            timer = threading.Timer(decay_interval,
                                    self.decay_weight,
                                    args=(node_id,
                                          self.store,
                                          decay_rate,
                                          decay_interval,
                                          self.q_scheduler))
            timer.start()

            # register timer
            self.timers_active[node_id] = timer

            logger.debug(f"Scheduled weight decay for node '{node_id}'")

    def enable(self):
        """ Start the weight decay scheduler in a new thread.
        """
        if self.enabled:
            return

        logger.debug("Starting weight decay scheduling daemon")
        self._thread = threading.Thread(target=self._schedule,
                                        name="weight_decay_scheduler")
        self._thread.start()

        self.enabled = True

    def disable(self):
        """ Terminate the weight decay scheduler and cancel all scheduled
            actions.
        """
        if not self.enabled:
            return

        logger.debug("Stopping weight decay scheduling daemon")
        self.q_scheduler.put(None)
        # stop timers and scheduler thread
        for timer in self.timers_active.values():
            timer.cancel()
            timer.join()

        self._thread.join()

        self.enabled = False

    def decay_weight(self, node_id: str, store: WeightStore,
                     decay_rate: float, decay_interval: float,
                     q_scheduler: Queue):
        weight_new = 0.
        with store._lock:
            rtime, weight_old = store.get(node_id)

            weight_new = weight_old * (1. - decay_rate)
            if weight_new <= 0.:
                store.deregister_node(node_id)
            else:
                if not store.update(node_id, weight_new, rtime):
                    # unable to update node state
                    return

        if weight_new > 0:
            q_scheduler.put((node_id, decay_rate, decay_interval))

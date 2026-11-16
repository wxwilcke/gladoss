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
from gladoss.modules.stream.info_elem import StreamInfoElement

logger = logging.getLogger(__name__)


class NodeProcessor(ReportProcessor):
    def __init__(self,
                 q_out: Queue,
                 q_shed: Queue,
                 rng: np.random.Generator,
                 cfg: SimpleNamespace,
                 **kwargs: dict | None):
        super().__init__(q_out=q_out, q_shed=q_shed,
                         rng=rng, cfg=cfg, kwargs=kwargs)

        # initiate store which will manage and track node health over time
        self.store = WeightStore(threading.RLock())

        # reception interval data
        self.strm_store = self.kwargs.get("strm_store", None)

        self.wd_scheduler = WeightDecayScheduler(self.store, self.q_shed)
        self.wd_scheduler.enable()

    def __del__(self):
        self.wd_scheduler.disable()

    def _compute_decay_interval(self, stream_id: str, rinterval_window: int)\
            -> float:
        """ Compute the weight decay interval by taking the average of the
            last n observed reception intervals from the given stream. Use
            a preset value if the reception interval is not available.

        :param stream_id: [TODO:description]
        :param rinterval_window: [TODO:description]
        :return: [TODO:description]
        """
        wd_interval = self.cfg.weight_decay_interval
        if self.strm_store is not None:
            wd_interval_lst = []
            with self.strm_store._lock:
                if stream_id in self.strm_store.nodes:
                    wd_interval_lst \
                        = self.strm_store.get(stream_id,
                                              StreamInfoElement.RINTERVAL,
                                              rinterval_window)

            if len(wd_interval_lst) > 0:
                wd_interval = sum(wd_interval_lst) / len(wd_interval_lst)

        return float(wd_interval)

    def process(self, report: ValidationReport):
        """ Assess node health by tracking violations through published
            validation reports about steams from that node.

            Upon receiving a report from an unknown node, a new node health
            state is registered and associated with that node through its
            identifier. Further violations (obtained from future reports about
            this node) will negatively affect the associated health state,
            whereas the absence of such violations will possitively affect this
            state (via a timed decay of negative state). A maintenance-request
            report will be send if the node state deteriorates too fast.

        :param report: [TODO:description]
        """
        thread_id = threading.current_thread().name

        node_id = report.node_id
        logger.info(f"Processing node (NID '{node_id}')")

        # register node if unknown
        with self.store._lock:
            if node_id not in self.store.nodes:
                logger.info(f"Registering new node (NID '{node_id}')")
                if not self.store.register_node(node_id):
                    # unable to register node
                    return
        logger.debug(f"Associated node state found (NID '{node_id}')")

        # compute weight decay interval
        # NOTE: this is computed for each stream; if a node holds multiple
        # streams this is thus computed multiple times for the same node.
        # This is usually not necessary. A solution is to keep track of
        # all streams of a node, but this requires assumptions on the streams'
        # characteristics. For now, we defer the problem to the scheduler, by
        # allowing only a single timer per node.
        rinterval_window = 4  # use average of past n events; arbitrary choice
        weight_decay_interval = self._compute_decay_interval(
                report.stream_id,
                rinterval_window)

        # schedule decay event
        # NOTE: this uses a preset decay rate, which may be too
        # simplistic when monitoring a heterogeneous set of devices. A better
        # solution would be to let the decay rate be determined by eg the
        # device type.  A less complex solution might be to allow these values
        # to be set per device (type) using a config file.
        logger.debug("Scheduling node health state update event "
                     f"(NID '{node_id}')")
        self.q_shed.put((node_id,
                         float(self.cfg.weight_decay_rate),
                         weight_decay_interval))

        # compute weight multiplier based on validation report
        weight_multiplier = computeWeightMultiplier(report, self.cfg)

        # calculate new weight and update node state
        logger.debug("Updating node health state from report "
                     f"(NID '{node_id}')")
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

        if violation_type is None:
            logger.debug(status_msg.capitalize() + f" (NID '{node_id}')")
        else:
            logger.info(status_msg.capitalize()
                        + f" detected (NID '{node_id}')")

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
                logger.debug("Cancelled scheduled node weight decay "
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
        """ Decay a registered node state weight and schedule
            a future decay event if the weight is positive.
            Otherwise, the node is deemed healthy and deregistered.

        :param node_id: [TODO:description]
        :param store: [TODO:description]
        :param decay_rate: [TODO:description]
        :param decay_interval: [TODO:description]
        :param q_scheduler: [TODO:description]
        """
        logger.debug("Running decay event for node health weight "
                     f"(NID '{node_id}')")
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

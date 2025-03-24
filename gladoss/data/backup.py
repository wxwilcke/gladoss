#!/usr/bin/env python

from datetime import timedelta
import logging
from pathlib import Path
import sched
from threading import Thread
from typing import Optional

from gladoss.core.pattern import PatternVault


logger = logging.getLogger(__name__)


class BackupManager():
    def __init__(self, pv: PatternVault, location: Path,
                 interval: Optional[timedelta] = None):
        """ Manage manual and automatic backups of patterns.

        :param pv: [TODO:description]
        :param location: [TODO:description]
        :param interval: [TODO:description]
        """
        self.pv = pv
        self.path = location
        self.interval = interval
        if type(self.interval) is timedelta:
            self.interval = self.interval.total_seconds()

        self.enabled = False

    def enable_auto_backup(self):
        if self.enabled or not isinstance(self.interval, int):
            return

        self.enabled = True

        self._scheduler = sched.scheduler()
        self._thread = Thread(target=self.create_auto_backup,
                              args=[self._scheduler])
        self._thread.start()
        logger.debug("Enabled auto backup")

    def disable_auto_backup(self):
        if not self.enabled:
            return

        self.enabled = False
        for event in self._scheduler.queue:
            self._scheduler.cancel(event)

        logger.debug("Disabled auto backup")

    def create_auto_backup(self, scheduler: sched.scheduler):
        if not isinstance(self.interval, int):
            return

        scheduler.enter(delay=self.interval, priority=1,
                        action=self.create_backup)
        while self.enabled:
            scheduler.run()

            scheduler.enter(delay=self.interval, priority=1,
                            action=self.create_backup)

    def create_backup(self):
        # TODO
        logger.info(f"Saved backup to {self.path}")
        pass

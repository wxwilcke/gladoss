#!/usr/bin/env python

import bz2
from datetime import datetime, timedelta
import logging
from pathlib import Path
import pickle
import sched
from threading import RLock, Thread
from typing import Optional

from gladoss.core.pattern import PatternVault


TIME_FORMAT = "%Y%m%dT%H%M%S"

logger = logging.getLogger(__name__)

# TODO: backup import


class BackupManager():
    def __init__(self, pv: PatternVault, location: Path, lock: RLock,
                 interval: Optional[timedelta] = None):
        """ Manage manual and automatic backups of patterns.

        :param pv: [TODO:description]
        :param location: [TODO:description]
        :param interval: [TODO:description]
        """
        self.pv = pv
        self.path = location
        self._lock = lock
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
        if not self.path.exists():
            self.path.mkdir()

        # generate filename based on current time
        filename = f"backup-{datetime.now().strftime(TIME_FORMAT)}.bak"

        self._lock.acquire()
        try:
            path = self.path / filename
            data = bz2.compress(pickle.dumps(self.pv))
            with open(path, 'wb') as f:
                pickle.dump(obj=data, file=f)

            logger.info(f"Saved backup to {path}")
        except Exception as err:
            logger.error(f"Unable to create backup: {err}")
        finally:
            self._lock.release()

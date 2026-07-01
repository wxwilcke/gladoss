#!/usr/bin/env python

import bz2
from datetime import datetime, timedelta
import logging
from pathlib import Path
import pickle
import sched
from threading import Thread
from typing import Optional

from gladoss.core.stores import Store


TIME_FORMAT = "%Y%m%dT%H%M%S"

logger = logging.getLogger(__name__)


class BackupManager():
    def __init__(self, location: Path,
                 stores: list[tuple[str, Store]] = list(),
                 interval: Optional[timedelta] = None):
        """ Manage manual and automatic backups of patterns.

        :param pv: [TODO:description]
        :param location: [TODO:description]
        :param interval: [TODO:description]
        """
        self.stores = stores
        self.path = location
        self.interval = interval
        if type(self.interval) is timedelta:
            self.interval = self.interval.total_seconds()

        self.enabled = False

    def enable_auto_backup(self):
        """ Enable the automatic backup scheduler. This will
            spawn a new thread that creates a backup every
            number of seconds, specified by the set interval.
        """
        if self.enabled or not isinstance(self.interval, int):
            return

        self.enabled = True

        self._scheduler = sched.scheduler()
        thread = Thread(target=self._create_auto_backup,
                        name="BackupManager",
                        args=[self._scheduler])
        thread.start()
        logger.debug("Enabled auto backup")

    def disable_auto_backup(self):
        """ Disable the automatic backup scheduler, if enabled.
        """
        if not self.enabled:
            return

        self.enabled = False
        for event in self._scheduler.queue:
            self._scheduler.cancel(event)

        logger.debug("Disabled auto backup")

    def _create_auto_backup(self, scheduler: sched.scheduler):
        """ Set up the backup scheduler. This procedure should
            only be called by the backup thread.

        :param scheduler: [TODO:description]
        """
        if not isinstance(self.interval, int):
            return

        scheduler.enter(delay=self.interval, priority=1,
                        action=self.create_backup)
        while self.enabled:
            scheduler.run()

            scheduler.enter(delay=self.interval, priority=1,
                            action=self.create_backup)

    def create_backup(self):
        """ Create a compressed backup of the pattern vault and
            write the result to disk. Use the current date and
            time to generate the file name.
        """
        if not self.path.exists():
            self.path.mkdir()

        # generate filename based on current time
        filename = f"backup-{datetime.now().strftime(TIME_FORMAT)}.bak"

        for _, store in self.stores:
            store._lock.acquire()

        try:
            path = self.path / filename
            with bz2.open(path, "wb") as f:
                f.write(pickle.dumps(obj=self.stores))

            logger.info(f"Saved backup to {path}")
        except Exception as err:
            logger.error(f"Unable to create backup: {err}")
        finally:
            for _, store in self.stores:
                store._lock.release()

    @staticmethod
    def restore_backup(filename: Path) -> list[tuple[str, Store]]:
        """ Restore a backup of a pattern vault instance by reading
            and decompressing the provided file. Raises an exception
            on failure.

        :param filename: [TODO:description]
        :return: [TODO:description]
        :raises Exception: [TODO:description]
        """
        assert filename.exists(), f"File '{filename.name}' cannot be found"

        stores = list()
        try:
            with bz2.open(filename.resolve(), "rb") as f:
                data = f.read()

            stores = pickle.loads(data)
            assert isinstance(stores, list) \
                and all([isinstance(a, str) and isinstance(b, Store)
                         for a, b in stores]), "Backup does not contain "\
                                               "expected data"
        except Exception as err:
            logger.error(f"Unable to restore backup: {err}")
            raise Exception(err)

        return stores

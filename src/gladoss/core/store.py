#! /usr/bin/env python

from __future__ import annotations

import logging
from threading import RLock

logger = logging.getLogger(__name__)


class Store:
    def __init__(self, lock: RLock) -> None:
        """ Thread safe object store with pickle support.

        The lock is omitted when pickling this class. A new
        lock needs to be assigned after restoration to facilitate
        thread safe use.

        :param lock: [TODO:description]
        """
        self._polytree = {}
        self._lock = lock

    def __getstate__(self):
        return {k: v for k, v in self.__dict__.items() if k != '_lock'}

    def __setstate__(self, state):
        self.__dict__.update(state)

    def __len__(self) -> int:
        with self._lock:
            return len(self._polytree.keys())

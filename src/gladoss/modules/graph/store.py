#! /usr/bin/env python

from __future__ import annotations

import bz2
import logging
import pickle
from datetime import datetime
from threading import RLock

from gladoss.core.store import Store
from gladoss.modules.graph.pattern import GraphPattern

logger = logging.getLogger(__name__)


class PatternVault(Store):
    def __init__(self, lock: RLock, compress: bool = True) -> None:
        super().__init__(lock)

        self.compress = compress

    def add_graph_pattern(self, pattern: GraphPattern) -> None:
        """ Add new graph pattern to pattern vault, by creating a
            new tree with the given pattern as root. This operation
            includes a timestamp to record the moment of creation,
            and is thread safe.

        :param pattern: [TODO:description]
        """
        key = pattern._id

        self._lock.acquire()
        try:
            assert key not in self._polytree
            self._polytree[key] = [(pattern, datetime.now())]
        except Exception as e:
            logger.error(f"Unable to add new pattern vault entry: {e}")
        finally:
            self._lock.release()

    def rmv_graph_pattern(self, pattern: GraphPattern) -> None:
        """ Remove registered graph pattern from the vault. This
            operation removes the entire tree and is thread safe.

        :param pattern: [TODO:description]
        """
        key = pattern._id

        self._lock.acquire()
        try:
            assert key in self._polytree
            del self._polytree[key]
        except Exception as e:
            logger.error(f"Unable to remove pattern vault entry: {e}")
        finally:
            self._lock.release()

    def prune_graph_pattern(self, pattern: GraphPattern | None) -> None:
        """ Prune the tree of the provided graph pattern by replacing
            the entire tree with a new tree that only contains the
            most recent graph pattern. Do this for all registered
            graph patterns if none is provided. This operation is
            thread safe

        :param pattern: [TODO:description]
        """
        prune_lst = [pattern]
        if pattern is None:
            prune_lst = [pattern._id for pattern in self._polytree]

        self._lock.acquire()
        try:
            for key in prune_lst:
                assert key in self._polytree
                self._polytree[key] = [self._polytree[key][-1]]
        except Exception as e:
            logger.error(f"Unable to prune pattern vault entry: {e}")
        finally:
            self._lock.release()

    def update_graph_pattern(self, pattern: GraphPattern,
                             rtime: datetime) -> None:
        """ Update registered graph pattern by adding the updated
            pattern as a new leaf to the tree. The previous version
            of the pattern automatically becomes a non-terminal
            vertex in the tree. This operation is thread safe.

        :param pattern: [TODO:description]
        """
        key = pattern._id

        self._lock.acquire()
        try:
            assert len(self._polytree[key]) > 0
            # compress old version
            if self.compress:
                prev, t_prev = self._polytree[key][-1]
                prev = bz2.compress(pickle.dumps(prev))
                self._polytree[-1] = (prev, t_prev)

            self._polytree[key].append((pattern, rtime))
        except Exception as e:
            logger.error(f"Unable to update pattern vault entry: {e}")
        finally:
            self._lock.release()

    def find_associated_graph_pattern(self, key: str)\
            -> GraphPattern | None:
        """ Find and return the most recent associated graph pattern. This
            operation is thread safe.

        :return: [TODO:description]
        """
        self._lock.acquire()
        try:
            pattern, _ = self._polytree[key][-1]
            return pattern
        except (KeyError, IndexError):
            return None
        finally:
            self._lock.release()

    def __repr__(self) -> str:
        return "PatternVault {" + ", ".join(list(self._polytree.keys())) + "}"

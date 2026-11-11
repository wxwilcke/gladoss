#! /usr/bin/env python

import logging
from datetime import datetime
from threading import RLock

from gladoss.core.store import Store

logger = logging.getLogger(__name__)


class WeightStore(Store):
    def __init__(self, lock: RLock, default_weight: float = 0.1) -> None:
        super().__init__(lock)

        self._default_weight = default_weight

    def register_node(self, node_id: str, weight_init: float | None = None)\
            -> bool:
        """ Register a new node, by creating a timestamped weight associated
            with its identifier. Return true if successful.

        :param node_id: [TODO:description]
        :return: [TODO:description]
        """
        if weight_init is None:
            weight_init = self._default_weight

        self._lock.acquire()
        try:
            assert node_id not in self._polytree, (
                    "Identifier already exists")

            self._polytree[node_id] = (datetime.now(), weight_init)
        except Exception as e:
            logger.error(f"Unable to register node '{node_id}': {e}")
            return False
        finally:
            self._lock.release()

        return True

    def deregister_node(self, node_id: str) -> bool:
        """ Remove a registered node from the polytree, by deleting the
            node and the weight attached to it. Return true if successful.

        :param node_id: [TODO:description]
        :return: [TODO:description]
        """
        self._lock.acquire()
        try:
            del self._polytree[node_id]
        except Exception as e:
            logger.error(f"Unable to deregister node '{node_id}': {e}")
            return False
        finally:
            self._lock.release()

        return True

    @property
    def nodes(self) -> set[str]:
        """ Return a set of all registered nodes.

        :param self [TODO:type]: [TODO:description]
        :return: [TODO:description]
        """
        with self._lock:
            return set(self._polytree.keys())

    def update(self, node_id: str, weight: float, rtime: datetime)\
            -> bool:
        self._lock.acquire()
        try:
            assert node_id in self._polytree

            self._polytree[node_id] = (rtime, weight)
        except Exception as e:
            logger.error(f"Unable to update weight for node '{node_id}': {e}")
            return False
        finally:
            self._lock.release()

        return True

    def get(self, node_id: str) -> tuple[datetime, float]:
        self._lock.acquire()

        data = None
        try:
            data = self._polytree[node_id]  # type: tuple[datetime, float]
        except Exception as e:
            logger.error(f"Unable to retrieve info for node '{node_id}': {e}")
        finally:
            self._lock.release()

        return data

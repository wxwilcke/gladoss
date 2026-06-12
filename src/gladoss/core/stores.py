#! /usr/bin/env python

from enum import Enum
import logging
from threading import RLock
from typing import Any, Optional


logger = logging.getLogger(__name__)


class MemoryStore():
    def __init__(self, lock: RLock) -> None:
        self._polytree = dict()
        self._lock = lock

    def register_node(self, node_id: str) -> bool:
        self._lock.acquire()
        try:
            if node_id not in self._polytree.keys():
                self._polytree[node_id] = dict()
        except Exception as e:
            logger.error(f"Unable to register node '{node_id}': {e}")
            return False
        finally:
            self._lock.release()

        return True

    def deregister_node(self, node_id: str) -> bool:
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
        with self._lock:
            return set(self._polytree.keys())

    def add(self, node_id: str, key: Enum, data: Any) -> bool:
        self._lock.acquire()
        try:
            tree = self._polytree[node_id]
            if key not in tree.keys():
                tree[key] = MemoryLinkedList()  # TODO Add decay

            tree[key].put(data)
        except Exception as e:
            logger.error(f"Unable to add key for node '{node_id}': {e}")
            return False
        finally:
            self._lock.release()

        return True

    def most_recent(self, node_id: str, key: Enum) -> Any:
        self._lock.acquire()

        data = None
        try:
            branch = self._polytree[node_id][key]
            data = branch.head
        except Exception as e:
            logger.error(f"Unable to retrieve info for node '{node_id}': {e}")
            return False
        finally:
            self._lock.release()

        return data

    def get(self, node_id: str, key: Enum, last_n: int = -1) -> list[Any]:
        self._lock.acquire()

        data_lst = list()
        try:
            branch = self._polytree[node_id][key]

            if last_n <= 0:
                # get all
                last_n = branch.memory_used

            data_lst = [item.data for item in branch.lastn(last_n)]
        except Exception as e:
            logger.error(f"Unable to retrieve info for node '{node_id}': {e}")
            return False
        finally:
            self._lock.release()

        return data_lst

    def get_tree(self, node_id: str) -> dict[str, list[Any]]:
        self._lock.acquire()

        data_dct = dict()
        try:
            tree = self._polytree[node_id]
            for key in tree.keys():
                branch = tree[key]
                data_dct[key] = [
                    item.data for item in branch.lastn(branch.memory_used)
                    ]
        except Exception as e:
            logger.error(f"Unable to retrieve info for node '{node_id}': {e}")
            return False
        finally:
            self._lock.release()

        return data_dct

    def __len__(self) -> int:
        return len(self.nodes)

    def __getstate__(self):
        return {k: v for k, v in self.__dict__.items() if k != '_lock'}

    def __setstate__(self, state):
        self.__dict__.update(state)


class MemoryLinkedList():
    class LinkedListNode():
        def __init__(self, data: Optional[Any],
                     prev: Optional[MemoryLinkedList.LinkedListNode] = None,
                     next: Optional[MemoryLinkedList.LinkedListNode] = None)\
                             -> None:
            self.data = data
            if prev is not None:
                self.prev = prev
            if next is not None:
                self.next = next

        def __repr__(self) -> str:
            return str(self.data)

    def __init__(self, memory: int = 100) -> None:
        self.memory_size = memory
        self.memory_used = 0

        self.head, self.tail = None, None

    def put(self, data: Any) -> None:
        node = MemoryLinkedList.LinkedListNode(data, self.head)

        if self.head is not None:
            self.head.next = node

        self.head = node
        if self.memory_used <= 0:
            self.tail = self.head

        self.memory_used += 1
        self._trim()

    def pop(self) -> Optional[MemoryLinkedList.LinkedListNode]:
        node = self.head
        if node is not None:
            self.head = node.prev
            if node.prev is not None:
                node.prev.next = None

            self.memory_used -= 1

        return node

    def lastn(self, n: int) -> list[LinkedListNode]:
        out = list()

        node = self.head
        for i in range(min(self.memory_used, n)):
            if node is None:
                break

            out.append(node)
            node = node.prev

        return out

    def _trim(self) -> None:
        while self.memory_used > self.memory_size:
            if self.tail is None:
                # this shouldn't happen; empty list?
                self.memory_used = 0

                break
            if self.head is self.tail:
                # only one node
                self.head, self.tail = None, None
                self.memory_used -= 1

                break

            tail = self.tail
            self.tail = tail.next
            self.tail.prev = None

            self.memory_used -= 1

    def __repr__(self) -> str:
        return f"MemoryLinkedList ({self.memory_used} / {self.memory_size})"

    def __len__(self) -> int:
        return self.memory_used

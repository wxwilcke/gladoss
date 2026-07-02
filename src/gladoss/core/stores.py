#! /usr/bin/env python

from __future__ import annotations
import bz2
from datetime import datetime
from enum import Enum
import logging
import pickle
from threading import RLock
from typing import Any, Optional

from gladoss.modules.graph.pattern import GraphPattern


logger = logging.getLogger(__name__)


class Store():
    def __init__(self, lock: RLock) -> None:
        """ Thread safe object store with pickle support.

        The lock is omitted when pickling this class. A new
        lock needs to be assigned after restoration to facilitate
        thread safe use.

        :param lock: [TODO:description]
        """
        self._polytree = dict()
        self._lock = lock

    def __getstate__(self):
        return {k: v for k, v in self.__dict__.items() if k != '_lock'}

    def __setstate__(self, state):
        self.__dict__.update(state)

    def __len__(self) -> int:
        with self._lock:
            return len(self._polytree.keys())


class MemoryStore(Store):
    def __init__(self, lock: RLock, memory: int = -1) -> None:
        """ The MemoryStore is a decaying polytree in which each tree is
            associated with a certain registered node, and in which branches
            are named linked lists. By registering nodes and adding data
            pertaining to a finite set of properties, this store maintains
            a memory of those data up to a certain specified number of entries.

            This class is thread safe.

        :param lock: [TODO:description]
        """
        super().__init__(lock)

        self.memory = memory

    def register_node(self, node_id: str) -> bool:
        """ Register a new node, by creating a tree associated with its
            identifier. Return true if successful.

        :param node_id: [TODO:description]
        :return: [TODO:description]
        """
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
        """ Remove a registered node from the polytree, by deleting its
            tree and all content attached to it. Return true if successful.

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

    def add(self, node_id: str, key: Enum, data: Any) -> bool:
        """ Add new data associated with a certain property about the given
            node, by adding the data to the linked list of that propery if
            it exists or by creating a new linked list otherwise. Returns true
            if successful.

        :param node_id: [TODO:description]
        :param key: [TODO:description]
        :param data: [TODO:description]
        :return: [TODO:description]
        """
        self._lock.acquire()
        try:
            tree = self._polytree[node_id]
            if key not in tree.keys():
                tree[key] = MemoryLinkedList(memory=self.memory)

            tree[key].put(data)
        except Exception as e:
            logger.error(f"Unable to add key for node '{node_id}': {e}")
            return False
        finally:
            self._lock.release()

        return True

    def most_recent(self, node_id: str, key: Enum) -> Any:
        """ Return the most recently added data element of a certain
            property from the given node. Return None on failure or if
            the property is unknown.

        :param node_id: [TODO:description]
        :param key: [TODO:description]
        :return: [TODO:description]
        """
        self._lock.acquire()

        data = None
        try:
            branch = self._polytree[node_id].get(key)
            if branch is not None:
                data = branch.head.data
        except Exception as e:
            logger.error(f"Unable to retrieve info for node '{node_id}': {e}")
        finally:
            self._lock.release()

        return data

    def get(self, node_id: str, key: Enum, last_n: int = -1) -> list[Any]:
        """ Return a list of the most recent n entries of this property from
            the given node. Returns all known entries if n is negative or if
            it exceeds the memory capacity of the underlying linked list.
            Returns an empty list on failure of if the property is unknown.

        :param node_id: [TODO:description]
        :param key: [TODO:description]
        :param last_n: [TODO:description]
        :return: [TODO:description]
        """
        self._lock.acquire()

        data_lst = list()
        try:
            branch = self._polytree[node_id].get(key)
            if branch is not None:
                if last_n <= 0:
                    # get all
                    last_n = branch.memory_used

                data_lst = [item.data for item in branch.lastn(last_n)]
        except Exception as e:
            logger.error(f"Unable to retrieve info for node '{node_id}': {e}")
        finally:
            self._lock.release()

        return data_lst

    def get_tree(self, node_id: str) -> dict[str, list[Any]]:
        """ Return the tree associated with the given node as a dictionary
            with named lists of data elements. Returns an empty dictionary
            on failure.

        :param node_id: [TODO:description]
        :return: [TODO:description]
        """
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
        finally:
            self._lock.release()

        return data_dct


class MemoryLinkedList():
    class LinkedListNode():
        def __init__(self, data: Optional[Any],
                     prev: Optional[MemoryLinkedList.LinkedListNode] = None,
                     next: Optional[MemoryLinkedList.LinkedListNode] = None)\
                             -> None:
            """ A node in a linked list with associated data and, optionally,
                a previous and next neighbour node.

            :param data: [TODO:description]
            :param prev: [TODO:description]
            :param next: [TODO:description]
            """
            self.data = data
            self.prev = prev
            self.next = next

        def __repr__(self) -> str:
            return str(self.data)

    def __init__(self, memory: int = -1) -> None:
        """ A list of linked nodes with, optionally, a specific maximum
            length (its memory). The most recently added node is the head,
            whereas the tail points to the oldest node. These point to
            the same node if the length of the list equals one, or to None
            if the list is empty.

        :param memory: [TODO:description]
        """
        self.memory_size = memory
        self.memory_used = 0

        self.head, self.tail = None, None

    def put(self, data: Any) -> None:
        """ Add a new node to the list. This will become the new head.

        :param data: [TODO:description]
        """
        node = MemoryLinkedList.LinkedListNode(data, self.head)

        if self.head is not None:
            self.head.next = node

        self.head = node
        self.memory_used += 1

        if self.memory_used == 1:
            self.tail = self.head

        self._trim()

    def pop(self) -> Optional[MemoryLinkedList.LinkedListNode]:
        """ Remove the most recently added node from the list and
            return it. This will move the head of the list back one
            node.

        :return: [TODO:description]
        """
        node = self.head
        if node is not None:
            self.head = node.prev
            if node.prev is not None:
                node.prev.next = None

            self.memory_used -= 1

        return node

    def lastn(self, n: int) -> list[LinkedListNode]:
        """ Return the most recent n nodes in the list. Returns
            the entire list if n is negative or if it exceeds the
            length of the list.

        :param n: [TODO:description]
        :return: [TODO:description]
        """
        out = list()

        node = self.head
        for i in range(min(self.memory_used, n)):
            if node is None:
                break

            out.append(node)
            node = node.prev

        return out

    def _trim(self) -> None:
        """ Trim the list to its specified maximum length by
            removing nodes from the tail forwards. This will
            change the tail node.
        """
        if self.memory_size < 0:
            # inf.
            return

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
        if self.memory_size >= 0:
            return ("MemoryLinkedList "
                    f"({self.memory_used} / {self.memory_size})")
        else:  # < 0
            return f"MemoryLinkedList ({self.memory_used} / inf.)"

    def __len__(self) -> int:
        return self.memory_used


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
            assert key not in self._polytree.keys()
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
            assert key in self._polytree.keys()
            del self._polytree[key]
        except Exception as e:
            logger.error(f"Unable to remove pattern vault entry: {e}")
        finally:
            self._lock.release()

    def prune_graph_pattern(self, pattern: Optional[GraphPattern]) -> None:
        """ Prune the tree of the provided graph pattern by replacing
            the entire tree with a new tree that only contains the
            most recent graph pattern. Do this for all registered
            graph patterns if none is provided. This operation is
            thread safe

        :param pattern: [TODO:description]
        """
        prune_lst = [pattern]
        if pattern is None:
            prune_lst = [pattern._id for pattern in self._polytree.keys()]

        self._lock.acquire()
        try:
            for key in prune_lst:
                assert key in self._polytree.keys()
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

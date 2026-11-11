#!/usr/bin/env python

import logging
from abc import ABC, abstractmethod
from collections.abc import Collection
from datetime import datetime
from functools import partial
from queue import Queue
from types import SimpleNamespace

import numpy as np
from rdf.graph import Statement

from gladoss.core.report import ValidationReport
from gladoss.core.utils import gen_id

logger = logging.getLogger(__name__)


class MessageProcessor(ABC):
    def __init__(self,
                 q_out: Queue,
                 q_shed: Queue,
                 rng: np.random.Generator,
                 cfg: SimpleNamespace):
        self.q_out = q_out
        self.q_shed = q_shed
        self.rng = rng
        self.cfg = cfg

        self.store = None

        # create callable to avoid importing numpy everywhere
        self.gen_id = partial(gen_id, self.rng)

    @abstractmethod
    def process(self,
                message: tuple[
                            tuple[str,
                                  str,
                                  Collection[Statement],
                                  int | list[int] | None
                                  ],
                            str,
                            datetime
                            ]):
        pass


class ReportProcessor(ABC):
    def __init__(self,
                 q_out: Queue,
                 q_shed: Queue,
                 rng: np.random.Generator,
                 cfg: SimpleNamespace):
        self.q_out = q_out
        self.q_shed = q_shed
        self.rng = rng
        self.cfg = cfg

        self.store = None

        # create callable to avoid importing numpy everywhere
        self.gen_id = partial(gen_id, self.rng)

    @abstractmethod
    def process(self,
                report: ValidationReport):
        pass

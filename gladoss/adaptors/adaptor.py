#!/usr/bin/env python

from abc import ABC, abstractmethod
import logging
from typing import Any, Optional, Self

from rdf.graph import Statement
from rdf.terms import Resource

logger = logging.getLogger(__name__)


class Adaptor(ABC):
    """ Abstract Base Class for adaptors. Subclass this class with a custom
        translation function to receive data from IoT devices via a RESTful API
        and to then convert these data to a corresponding RDF graph.
    """

    def __init__(self: Self, **kwargs: Optional[str]) -> None:
        self.context = kwargs
        self.init_hook()

    @abstractmethod
    def init_hook(self: Self) -> None:
        """ Execute additional commands on initialisation.
        """
        pass

    @abstractmethod
    def get_anchors(self: Self, data: dict[str, Any],
                    **kwargs: Optional[str]) -> list[Resource]:
        pass

    @abstractmethod
    def set_headers(self: Self, **kwargs: Optional[str]) -> dict[str, Any]:
        pass

    @abstractmethod
    def set_payload(self: Self, **kwargs: Optional[str]) -> dict[str, Any]:
        pass

    @abstractmethod
    def translate(self: Self, data: dict[str, Any],
                  **kwargs: Optional[str]) -> list[Statement]:
        """ Translate the received data to RDF.

        :param data: data received from API
        :param kwargs: optional keyword arguments
        :return: a list with statements and anchors
        """
        pass

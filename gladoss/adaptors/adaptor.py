#!/usr/bin/env python

from abc import ABC, abstractmethod
import logging
from threading import Event
from types import SimpleNamespace
from typing import Any, Self

from rdf.graph import Statement

logger = logging.getLogger(__name__)


class Adaptor(ABC):
    """ Abstract Base Class for adaptors. Subclass this class with a custom
        translation function to receive data from IoT devices via a RESTful API
        and to then convert these data to a corresponding RDF graph.
    """

    def __init__(self: Self, controller: Event,
                 config: SimpleNamespace) -> None:

        self.config = config
        self._controller = controller

        self.context = dict()  # share data between functions
        self.connectors = set()

        self.init_hook()

        self.add_connectors()

    @abstractmethod
    def add_connectors(self: Self) -> None:
        """ Add connectors to adaptor, one for each different endpoint
            or request type.
        """
        pass

    @abstractmethod
    def init_hook(self: Self) -> None:
        """ Execute additional commands on initialisation.
        """
        pass

    @abstractmethod
    def cleanup_hook(self: Self) -> None:
        """ Execute additional commands on exit.
        """
        pass

    @abstractmethod
    def set_headers(self: Self) -> dict[str, Any]:
        """ Returns headers for polling the endpoint. Defaults
            to empty headers

        :param self: [TODO:description]
        :return: [TODO:description]
        """
        return dict()

    @abstractmethod
    def set_payload(self: Self) -> dict[str, Any]:
        """ Returns payload for polling the endpoint. Defaults
            to empty payload.

        :param self: [TODO:description]
        :return: [TODO:description]
        """
        return dict()

    @abstractmethod
    def set_report_headers(self: Self, data: dict[str, Any])\
            -> dict[str, Any]:
        """ Returns headers for publishing the validation report
            to the endpoint. Defaults to empty headers

        :param self: [TODO:description]
        :return: [TODO:description]
        """
        return dict()

    @abstractmethod
    def set_report_payload(self: Self, data: dict[str, Any])\
            -> dict[str, Any]:
        """ Returns payload for publishing the validation report
            to the endpoint. Defaults to empty payload.

        :param self: [TODO:description]
        :return: [TODO:description]
        """
        return dict()

    @abstractmethod
    def set_receipt_headers(self: Self, data: dict[str, Any])\
            -> dict[str, Any]:
        """ Returns headers for sending a receipt. Defaults
            to empty headers.

        :param self: [TODO:description]
        :return: [TODO:description]
        """
        return dict()

    @abstractmethod
    def set_receipt_payload(self: Self, data: dict[str, Any])\
            -> dict[str, Any]:
        """ Returns payload for sending a receipt. Defaults
            to empty payload.

        :param self: [TODO:description]
        :return: [TODO:description]
        """
        return dict()

    @abstractmethod
    def translate(self: Self, data: dict[str, Any])\
            -> list[tuple[str, list[Statement]]]:
        """ Translate the received data to RDF.

        :param data: data received from API
        :param kwargs: optional keyword arguments
        :return: a list with statements and their identifier
        """
        return list()

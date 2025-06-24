#! /usr/bin/env python

from ast import literal_eval
from datetime import datetime
import logging

from gladoss.core.multimodal.timeutils import cast_datefrag, cast_datefrag_rev, cast_datetime, cast_datetime_rev
from rdf.namespaces import XSD
from rdf.terms import Literal, IRIRef


XSD_DATEFRAG = {XSD + 'gDay',
                XSD + 'gMonth',
                XSD + 'gMonthDay'}

XSD_DATETIME = {XSD + 'date',
                XSD + 'dateTime',
                XSD + 'dateTimeStamp',
                XSD + 'gYear',
                XSD + 'gYearMonth'}

XSD_NUMERIC = {XSD + 'decimal',
               XSD + 'double',
               XSD + 'float',
               XSD + 'long',
               XSD + 'int',
               XSD + 'short',
               XSD + 'byte',
               XSD + 'integer',
               XSD + 'nonNegativeInteger',
               XSD + 'nonPositiveInteger',
               XSD + 'negativeInteger',
               XSD + 'positiveInteger',
               XSD + 'unsignedLong',
               XSD + 'unsignedInt',
               XSD + 'unsignedShort',
               XSD + 'unsignedByte'}

XSD_STRING = {XSD + 'string',
              XSD + 'normalizedString',
              XSD + 'token',
              XSD + 'language',
              XSD + 'Name',
              XSD + 'NCName',
              XSD + 'ENTITY',
              XSD + 'ID',
              XSD + 'IDREF',
              XSD + 'NMTOKEN',
              XSD + 'anyURI'}

XSD_CONTINUOUS = {XSD + 'date',
                  XSD + 'dateTime',
                  XSD + 'dateTimeStamp',
                  XSD + 'decimal',
                  XSD + 'double',
                  XSD + 'float'}

XSD_DISCRETE = set.union(XSD_DATEFRAG,
                         XSD_DATETIME,
                         XSD_NUMERIC,
                         XSD_STRING) - XSD_CONTINUOUS

EPOCH_TIME = datetime(year=1970, month=1, day=1, hour=1)
DAYS_PER_YEAR = 365


logger = logging.getLogger(__name__)


def infer_datatype(literal: Literal) -> IRIRef:
    """ Infer XSD datatype from semantic annotations.
        Falls back to python heuristic. Defaults to
        string.

    :param literal: [TODO:description]
    :return: [TODO:description]
    """
    dtype = literal.datatype
    if literal.language is not None:
        dtype = XSD + "string"

    if dtype is None:
        # fallback to python
        dtype = infer_python_type(literal.value)

    return dtype


def infer_python_type(s: str) -> IRIRef:
    """ Infer XSD datatype heuristically. Defaults back
        to string.

    :param s: [TODO:description]
    :return: [TODO:description]
    """
    xsd_type = XSD + 'string'
    try:
        dtype = type(literal_eval(s))

        if dtype is int:
            xsd_type = XSD + 'integer'
        elif dtype is float:
            xsd_type = XSD + 'float'
    except (ValueError, SyntaxError):
        pass

    return xsd_type


def cast_literal(dtype: IRIRef | None, value: Literal) -> str | int | float:
    """ Cast literal value to appropriate python object
        based on given XSD datatype. Compound values 'X-Y'
        (eg gMonthDay) are consolidated into units of Y
        (ie days), and full dates with/without time component
        are converted to unix timestamps.

    :param dtype: [TODO:description]
    :param value: [TODO:description]
    :return: [TODO:description]
    """
    value = str(value)
    if dtype is not None:
        try:
            if dtype in XSD_DATETIME:
                value = float(cast_datetime(dtype, value))
            elif dtype in XSD_DATEFRAG:
                value = int(cast_datefrag(dtype, value))
            elif dtype in XSD_CONTINUOUS:
                value = float(value)
            elif dtype in XSD_DISCRETE & XSD_NUMERIC:
                value = int(value)
        except ValueError:
            logger.debug(f"Error when trying to cast literal value '{value}'"
                         + f" of type {dtype}.")

    return value


def cast_literal_rev(value: str | int | float,
                     dtype: IRIRef | None, lang: str | None) -> Literal:
    if dtype is not None:
        try:
            if dtype in XSD_DATETIME:
                value = cast_datetime_rev(dtype, value)  # str
            elif dtype in XSD_DATEFRAG:
                value = cast_datefrag_rev(dtype, value)  # str
        except ValueError:
            logger.debug(f"Error when trying to inverse cast literal value "
                         f" '{value}' of type {dtype}.")

    value = Literal(str(value), datatype=dtype, language=lang)

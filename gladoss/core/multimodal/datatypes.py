#! /usr/bin/env python

from ast import literal_eval
from datetime import datetime
import logging

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


def cast_literal(dtype: IRIRef, value: str) -> str | int | float:
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
    try:
        if dtype in {XSD + 'date', XSD + 'dateTime'}:
            value = datetime.fromisoformat(value)
        elif dtype == XSD + 'gMonthDay':
            # return as number of days
            m, d = value.split('-')
            v = datetime(year=1970, month=int(m), day=int(d), hour=1)

            value = (v - EPOCH_TIME).days
        elif dtype == XSD + 'gYearMonth':
            # return as number of months
            y, m = value.split('-')
            v = datetime(year=int(y), month=int(m), day=1, hour=1)

            value = abs((v - EPOCH_TIME).days * DAYS_PER_YEAR)
        elif dtype in XSD_CONTINUOUS:
            value = float(value)
        elif dtype in XSD_DISCRETE & XSD_NUMERIC:
            value = int(value)
    except ValueError:
        logger.debug(f"Error when trying to cast literal value '{value}'"
                     + f" of type {dtype}.")
        pass

    return value

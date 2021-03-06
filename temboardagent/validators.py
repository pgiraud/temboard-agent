# User Input Validator
#
# These functions provide stateless validation of user input, mainly CLI
# arguments and environment variables.
#
# On invalid input, a ValueError is raised. Other exceptions are considered a
# bug.
#
# A validator is idempotent. It must accepts what it returns.

import json
import logging
import os.path
import re
from distutils.util import strtobool
from logging.handlers import SysLogHandler

from .log import HANDLERS as LOG_METHODS


_address_re = re.compile(
    r'(?:[3-9]\d?|2(?:5[0-5]|[0-4]?\d)?|1\d{0,2}|\d)'
    r'(\.(?:[3-9]\d?|2(?:5[0-5]|[0-4]?\d)?|1\d{0,2}|\d'
    r')){3}$'
)


def address(raw):
    if not _address_re.match(raw):
        raise ValueError('invalid address')
    return raw


def boolean(raw):
    if raw in (True, False):
        return raw

    return bool(strtobool(raw))


def dir_(raw):
    raw = os.path.realpath(raw)
    if not os.path.isdir(raw):
        raise ValueError('Not a directory')
    return raw


def file_(raw):
    raw = os.path.realpath(raw)
    if not os.path.exists(raw):
        raise ValueError('%s: File not found' % raw)
    return raw


_identifier_re = re.compile(r'^[a-zA-Z0-9]+$')


def jsonlist(raw):
    if hasattr(raw, 'lower'):
        raw = json.loads(raw)

    if not isinstance(raw, list):
        raise ValueError('not a list')

    raw = [str(e) for e in raw]
    for entry in raw:
        if not _identifier_re.match(entry):
            raise ValueError('%s is invalid' % entry)

    return raw


def port(raw):
    port = int(raw)

    if 0 > port or port > 65635:
        raise ValueError('Port out of range')

    return port


def loglevel(raw):
    raw = raw.upper()
    if raw not in logging._levelNames:
        raise ValueError('unkown log level')
    return raw


def logmethod(raw):
    if raw not in LOG_METHODS:
        raise ValueError('unkown method')
    return raw


def syslogfacility(raw):
    if raw not in SysLogHandler.facility_names:
        raise ValueError('unkown syslog facility')
    return raw


def writeabledir(raw):
    raw = dir_(raw)
    if not os.access(raw, os.W_OK):
        raise ValueError('Not writable')
    return raw

"""
Utilities to resolve a string to Mongo host, or a Argus library.
"""
import logging
import re
from weakref import WeakValueDictionary

import six

__all__ = ["get_argus_lib"]

logger = logging.getLogger(__name__)

# Application environment variables
argus_cache = WeakValueDictionary()

CONNECTION_STR = re.compile(r"(^\w+\.?\w+)@([^\s:]+:?\w+)$")


def get_argus_lib(connection_string, **kwargs):
    """
    Returns a mongo library for the given connection string

    Parameters
    ---------
    connection_string: `str`
        Format must be one of the following:
            library@trading for known mongo servers
            library@hostname:port

    Returns:
    --------
    Argus library
    """
    m = CONNECTION_STR.match(connection_string)
    if not m:
        raise ValueError("connection string incorrectly formed: %s" % connection_string)
    library, host = m.group(1), m.group(2)
    return _get_argus(host, **kwargs)[library]


def _get_argus(instance, **kwargs):
    # Consider any kwargs passed to the Argus as discriminators for the cache
    key = instance, frozenset(six.iteritems(kwargs))

    # Don't create lots of Argus instances
    argus = argus_cache.get(key, None)
    if not argus:
        # Create the instance. Note that Argus now connects
        # lazily so this doesn't connect until on creation.
        from .argus import Argus

        argus = Argus(instance, **kwargs)
        argus_cache[key] = argus
    return argus

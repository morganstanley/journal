"""ZooKeeper helper functions.
"""

import logging
import urllib

import kazoo
import kazoo.client
import kazoo.exceptions
import kazoo.security
from kazoo.client import (
    NodeExistsError,
    NoNodeError,
    ConnectionClosedError,
)


from . import client as zkclient

_LOG = logging.getLogger(__name__)


###############################################################################
def connect(zkurl, **connargs):
    """Establish connection with Zk and return an instance of a KazooClient.

    """
    _LOG.debug('Connecting to %s', zkurl)

    # Use scheme to parse zkurl and extract chroot, hosts, etc. E.g.:
    # zookeeper+sasl://host:port,host:port,host:port/foo#mechanism=gssapi
    zkclient_cls, url_connargs = _parse_zkurl(zkurl)

    client_connargs = {
        'hosts': [],
        'auth_data': [],
    }
    # Merge in what we parsed from the zkurl
    client_connargs.update(url_connargs)
    # Merge in the provided overrides.
    client_connargs.update(connargs)

    _LOG.debug(
        'Connecting to zookeeper: [%s:%s]: %r',
        zkclient_cls.__module__,
        zkclient_cls.__name__,
        client_connargs
    )
    chroot = client_connargs.pop('chroot')
    zkc = zkclient_cls(**client_connargs)
    zkc.chroot = chroot
    return zkc


def make_anonymous_acl(perm):
    """Constructs anonymous (anyone) acl."""
    if not perm:
        perm = 'r'

    return kazoo.security.make_acl(
        'world', 'anyone',
        read='r' in perm,
        write='w' in perm,
        create='c' in perm,
        delete='d' in perm,
        admin='a' in perm
    )


def _parse_zkurl(zkurl):
    """
    """
    parsed = urllib.parse.urlparse(zkurl)
    # Load a handler for the scheme
    scheme_mod = zkclient.get_scheme_module(parsed.scheme)

    zkclient_cls = scheme_mod.ZkClient
    connargs = scheme_mod.url_connargs(parsed)

    return zkclient_cls, connargs


__all__ = (
    'connect',
    'ConnectionClosedError',
    'make_anonymous_acl',
    'NodeExistsError',
    'NoNodeError',
)

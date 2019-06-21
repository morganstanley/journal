"""Vanila no auth zookeeper connection.
"""

import kazoo.client

from .. import utils as zkutils
from . import _base_client


class ZkClient(_base_client.BaseZkClient, kazoo.client.KazooClient):
    """
    zookeeper ZkClient class
    """
    @property
    def identity(self):
        return 'anyone'

    def make_identity_acl(self, identity, perm):
        del identity
        return zkutils.make_anonymous_acl(perm)

    def make_self_acl(self, perm):
        return zkutils.make_anonymous_acl(perm)

    def make_user_acl(self, user, perm):
        """
        make user acl
        """
        del user
        return zkutils.make_anonymous_acl(perm)


def url_connargs(parsed_zkurl):
    """
    :param  `~urllib.parse.ParseResult` parsed_zkurl:
        A `urlparse()`'d zkurl
    :returns:
        ``dict`` - Connargs for the `~ZKClient` constructor
    """
    if parsed_zkurl.username:
        # We need to strip user/pass data from netloc
        netloc = parsed_zkurl.netloc.split('@', 1)[1]
    else:
        netloc = parsed_zkurl.netloc
    hosts = netloc.split(',')

    return {
        'hosts': hosts,
        'auth_data': [],
        'chroot': parsed_zkurl.path
    }


__all__ = (
    'url_connargs',
    'ZkClient'
)

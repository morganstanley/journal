"""
ZkClient Base class
"""
import abc


class BaseZkClient(metaclass=abc.ABCMeta):
    """
    Abstract class for ZkClient
    """
    @abc.abstractproperty
    def identity(self):
        """The current identity of the client user.
        """

    @abc.abstractmethod
    def make_identity_acl(self, identity, perm):
        """Constructs an ACL based on user and permissions.
        :param ``str`` user:
            Identity which will be granted the perm (scheme specific).
        """
        del identity, perm

    @abc.abstractmethod
    def make_self_acl(self, perm):
        """Constucts acl for the client current identity.
        """
        del perm


__all__ = (
    'BaseZkClient'
)

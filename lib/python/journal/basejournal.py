"""
Base module for journal
"""
import abc


class BaseJournal(metaclass=abc.ABCMeta):
    """
    All custom journal modules should implement this class
    """

    @abc.abstractmethod
    def write(self, txid, step, msg):
        """
        Write msg to journal based on txid and step.
        Within a transaction there can be multiple steps
        which you want to journal
        """

    @abc.abstractmethod
    def status(self, txid):
        """
        Get status of a transaction
        """


__all__ = (
    'BaseJournal'
)

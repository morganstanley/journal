"""
Journal Factory module
"""
import logging
import http.client
import sys
from journal import nfsjournal
from journal import zkjournal

_LOG = logging.getLogger(__name__)


class Journal():
    """
    Creates journal objects for
    primary and secondary
    """
    def __init__(self, jconfig, kwargs):
        """
        Constructor for journal
        """
        self.primary = None
        self.secondary = None

        self.initialize(jconfig, kwargs)

    def initialize(self, jconfig, kwargs):
        """
        creates journal objects
        """
        cachesize = jconfig.get('cachesize', None)
        adminuser = jconfig.get('adminuser', None)
        if 'primary' in jconfig:
            self.primary = self.create_journal(jconfig['primary'],
                                               kwargs, cachesize,
                                               adminuser)
        if 'secondary' in jconfig:
            self.secondary = self.create_journal(jconfig['secondary'])

    def create_journal(self, jconf, kwargs=None,
                       cachesize=None, adminuser=None):
        """get the name and create obj"""
        (jmodule, jval) = jconf.split('://')
        str(jmodule).lower()
        if jmodule == 'nfs':
            return nfsjournal.NFSJournal(jval)
        if 'zookeeper' in jmodule:
            return zkjournal.ZookeeperJournal(jconf,
                                              kwargs,
                                              adminuser,
                                              cachesize)
        sys.exit("Unsupported journal type")

    def write(self, txid, step, msg):
        """
        Write journal to primary or secondary
        """
        # Primary journaling
        rc = 0
        if self.primary is not None:
            rc = self.primary.write(txid, step, msg)
        # primary journal is not set
        else:
            rc = 1
        # Secondary journaling (failover)
        # We are here because 'primary' failed or
        # primary is set to None
        if self.secondary is not None and rc != 0:
            rc = self.secondary.write(txid, step, msg)
        return rc

    def status(self, txid):
        """
        Get status from primary or secondary
        """
        # Primary journaling
        code = None
        if self.primary is not None:
            (resp, code) = self.primary.status(txid)
        # Secondary journaling (failover)
        # We are here because 'primary' failed
        if code is None and self.secondary is not None:
            (resp, code) = self.secondary.status(txid)
        if code is None:
            _LOG.error('task %r not found', txid)
            actual_data = 'Task not found'
            resp = {'status': actual_data}
            code = http.client.NOT_FOUND
        if code == http.client.PROCESSING:
            actual_data = 'Task in progress'
            resp = {'status': actual_data}
        return (resp, code)

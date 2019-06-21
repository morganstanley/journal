"""
Module for NFS journal
"""

import errno
import http.client
import json
import os
import logging
import tempfile
from journal import basejournal

_LOG = logging.getLogger(__name__)


class NFSJournal(basejournal.BaseJournal):
    """
    Class defining nfs journal
    """
    def __init__(self, opt):
        """
        constructor for nfs journal
        """
        self.nfspath = opt

    def write(self, txid, step, msg):
        """
        Function to write journal to NFS
        """
        filename = '{0}_{1}'.format(txid, step)
        rc = 0
        journal_file = os.path.join(self.nfspath, filename)
        try:
            with tempfile.NamedTemporaryFile(
                    suffix='-XXXXX.tmp',
                    dir=self.nfspath,
                    delete=False, mode='w'
            ) as outfile:
                json.dump(msg, outfile)
        except (IOError, OSError):
            _LOG.exception('Error writing to nfs primary journal')
            rc = 1
        else:
            os.rename(outfile.name, journal_file)
        return rc

    def status(self, txid):
        """
        Function to get status of a txid
        """
        commitnode = os.path.join(self.nfspath,
                                  '{0}_{1}'.format(txid, 'commit'))
        abortnode = os.path.join(self.nfspath,
                                 '{0}_{1}'.format(txid, 'abort'))
        beginnode = os.path.join(self.nfspath,
                                 '{0}_{1}'.format(txid, 'begin'))
        try:
            with open(commitnode, 'r') as fin:
                actual_data = fin.read()
                final_resp = {'status': json.loads(actual_data)}
                return (final_resp, http.client.OK)
        except OSError as err:
            if err.errno == errno.ENOENT:
                # File doesn't exist
                _LOG.debug('No commit status for task %r', txid)
        try:
            with open(abortnode, 'r') as fin:
                actual_data = fin.read()
                final_resp = {'status': json.loads(actual_data)}
                return (final_resp, http.client.OK)
        except OSError as err:
            if err.errno == errno.ENOENT:
                # File doesn't exist
                _LOG.debug('No abort status for task %r', txid)
        if os.path.exists(beginnode):
            final_resp = None
            return (final_resp, http.client.PROCESSING)
        else:
            return (None, None)

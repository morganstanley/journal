"""
Main module to resync journal in NFS
to zookeeper
"""

import os
import time
import logging
import fcntl
import errno
import json
import sys
import yaml
import kazoo.exceptions
from journal import zkjournal

_LOG = logging.getLogger(__name__)


def resync_with_nfs(zkclient, nfs_path):
    """
    Main function responsible for resync
    """
    lockfile = os.path.join(nfs_path, '.lock')
    with open(lockfile, 'w') as f:
        try:
            fcntl.lockf(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            for journalfile in os.listdir(nfs_path):
                if not journalfile.startswith('.'):
                    jfile = os.path.join(nfs_path, journalfile)
                    try:
                        if os.path.isfile(jfile) is True:
                            txid, step = journalfile.split('_')
                            with open(jfile, 'r') as fin:
                                msg = json.load(fin)
                    except IOError:
                        _LOG.exception("""Error with data in
                                       journal entry file %s""", jfile)
                        continue
                    else:
                        rc = zkclient.write(txid, step, msg)
                        if rc == 1:
                            continue
                        else:
                            os.remove(jfile)
        except IOError as err:
            if err.errno == errno.EAGAIN:
                # this is a lock fail, just skip
                pass
            else:
                # this is another file operation exception
                _LOG.exception('Exception while trying to lockfile: %s',
                               lockfile)
                return


def main(adminuser=None, cfg=None, primary=None, secondary=None):
    """
    Based on command line arguments, resync nfs to zookeeper.
    Resync happens every minute.
    """
    _LOG.info('current parent process id %d', os.getpid())
    kwargs = dict()
    journal_nfspath = None
    if cfg:
        with open(cfg) as config:
            jconf = yaml.load(config)
            if 'primary' in jconf:
                primary = jconf['primary']
                jconf.pop('primary')
            if 'secondary' in jconf:
                secondary = jconf['secondary']
            kwargs = jconf
    if primary is None:
        sys.exit("Missing primary journal")
    if 'zookeeper' not in primary:
        sys.exit("Wrong zookeeper information")
    (jmodule, jval) = secondary.split('://')
    str(jmodule).lower()
    if 'nfs' in jmodule:
        journal_nfspath = jval
    if journal_nfspath and primary:
        start_resync(primary, kwargs, journal_nfspath, adminuser)
    else:
        sys.exit('Error in Journal config')


def start_resync(zkurl, kwargs, journal_nfspath, adminuser=None):
    """
    Start resync with nfs
    """
    zkj = zkjournal.ZookeeperJournal(zkurl, kwargs, adminuser)
    while True:
        try:
            if zkj.zk.connected:
                resync_with_nfs(zkj, journal_nfspath)
            else:
                zkj.journal_zk_start()
        except kazoo.exceptions.SessionExpiredError as err:
            _LOG.exception('Error - %s', err)
        except kazoo.exceptions.KazooException as err:
            _LOG.exception('Error - %s', err)
        except kazoo.handlers.threading.KazooTimeoutError as err:
            _LOG.exception('Error - %s', err)
        time.sleep(60)

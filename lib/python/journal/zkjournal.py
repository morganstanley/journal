"""
Module for zookeeper journal
"""
import csv
import errno
import fcntl
import functools
import glob
import gzip
import http.client
import json
import logging
import os
import re
import shutil
import sqlite3
import sys
import time
import zlib
import kazoo.exceptions
from kazoo.client import KazooState
from journal import basejournal
from journal.zk import utils as zkutils

_LOG = logging.getLogger(__name__)

HISTORY_CACHE = {}

# Max zk sequence number is 2**32 (signed integer)
# SERIAL_BITS defines the size of sliding window
SERIAL_BITS = 32

SQLITE_CREATE = """
      CREATE TABLE IF NOT EXISTS journal (
          id             INTEGER PRIMARY KEY AUTOINCREMENT,
          date           DATETIME DEFAULT CURRENT_TIMESTAMP,
          authuser_id    VARCHAR(64)   NOT NULL,
          user_id        VARCHAR(64)   NOT NULL,
          as_role        VARCHAR(16)   NULL,
          request_id     VARCHAR(36)   NOT NULL,
          transaction_id VARCHAR(36)   NOT NULL,
          step           VARCHAR(16)   NOT NULL,
          host           VARCHAR(254)  NOT NULL,
          resource       VARCHAR(64)   NOT NULL,
          resourcegroup  VARCHAR(64)   NOT NULL,
          verb           VARCHAR(64)   NOT NULL,
          resourcepk     VARCHAR(128)  NULL,
          payload        TEXT          NULL,
          cm             VARCHAR(20)   NULL
      )"""

SQLITE_INSERT = """
      INSERT INTO journal (
          host, authuser_id, user_id, date,
          request_id, transaction_id,
          step, as_role,
          resourcegroup, resource, verb, resourcepk,
          payload, cm
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

SQLITE_SELECT = """
    SELECT host, authuser_id, user_id, date,
    request_id, transaction_id, step, as_role,
    resourcegroup, resource, verb, resourcepk, payload, cm
    FROM journal WHERE request_id=? AND
    step=?
    """

SQLITE_SELECT_ALL = """
    SELECT host, authuser_id, user_id, date,
    request_id, transaction_id, step, as_role,
    resourcegroup, resource, verb, resourcepk, payload, cm
    FROM journal
    """

CSV_COLUMNS = ['transaction_id',
               'request_id',
               'step',
               'host',
               'resource',
               'verb',
               'pk',
               'date',
               'user_id',
               'authuser_id',
               'role',
               'cm',
               'payload']

SQLITE_NODE_REGEX = re.compile(r'^sqlite-db#(-?\d+)$')


class ZookeeperJournal(basejournal.BaseJournal):
    """
    Class responsible for zookeeper client instance
    """

    def __init__(self, zkurl, kwargs, adminuser=None, cachesize=None):
        """
        Create zookeeper client instance and acl.
        """
        self.cachesize = cachesize
        self.zk = zkutils.connect(zkurl, **kwargs)
        self.zk.add_listener(self.my_listener)
        selfperm = 'rwc'
        if not adminuser:
            selfperm = 'rwcda'
        self.acl = [
            self.zk.make_self_acl(selfperm),
            zkutils.make_anonymous_acl('r')
        ]
        if adminuser:
            self.acl.append(self.zk.make_user_acl(adminuser, 'rwcda'))

    def journal_zk_start(self):
        """
        Start zookeeper client based on
        zookeeper state
        """
        try:
            if self.zk.state == 'LOST':
                self.zk.start(timeout=1)
            if self.zk.state == 'SUSPENDED':
                self.zk.stop()
        except kazoo.exceptions.SessionExpiredError:
            _LOG.exception('Zookeeper down - session expired')
            return
        except kazoo.exceptions.KazooException:
            _LOG.exception('Error starting zookeeper primary journal')
            return
        except kazoo.handlers.threading.KazooTimeoutError as err:
            _LOG.exception('Error zookeeper - %s', err)
            return
        else:
            _LOG.info('Zookeeper started')
            if not self.zk.exists('/'):
                sys.exit('Chroot {0} doesn\'t exist'.format(self.zk.chroot))

    def my_listener(self, state):
        """
        Zookeeper client instance watcher
        """

        if state == KazooState.LOST:
            # Register somewhere that the session was lost
            _LOG.info('KazooState.LOST')
        if state == KazooState.SUSPENDED:
            # Handle being disconnected from Zookeeper
            _LOG.info('KazooState.SUSPENDED')
        if state == KazooState.CONNECTED:
            # Handle being connected/reconnected to Zookeeper
            _LOG.info('KazooState.CONNECTED')

    def write(self, txid, step, msg):
        """
        This function write journal to zookeeper
        """
        childnode = '/{0}/{1}'.format(txid, step)
        rc = 0
        if not self.zk.connected:
            self.journal_zk_start()
        try:
            json_msg = json.dumps(msg)
            compressed_msg = zlib.compress(json_msg.encode())
            if self.zk.connected:
                self.zk.create(childnode, value=compressed_msg,
                               makepath=True, acl=self.acl)
            else:
                rc = 1
        except kazoo.exceptions.NodeExistsError:
            return rc
        except kazoo.exceptions.KazooException:
            _LOG.exception('Error writing to zookeeper primary journal')
            rc = 1
        except kazoo.handlers.threading.KazooTimeoutError as err:
            _LOG.exception('Zookeeper timed out - %s', err)
            rc = 1
        return rc

    def status(self, txid):
        """
        Function to get status of a txid
        """
        if not self.zk.connected:
            self.journal_zk_start()
        if not self.zk.connected:
            return (None, None)
        commitnode = '/{0}/{1}'.format(txid, 'commit')
        abortnode = '/{0}/{1}'.format(txid, 'abort')
        beginnode = '/{0}/{1}'.format(txid, 'begin')
        try:
            if self.zk.exists(commitnode):
                data = self.zk.get(commitnode)
                actual_data = zlib.decompress(data[0]).decode()
                final_resp = {'status': json.loads(actual_data)}
                return (final_resp, http.client.OK)
            if self.zk.exists(abortnode):
                data = self.zk.get(abortnode)
                actual_data = zlib.decompress(data[0]).decode()
                final_resp = {'status': json.loads(actual_data)}
                return (final_resp, http.client.OK)
            if self.zk.exists(beginnode):
                final_resp = None
                return (final_resp, http.client.PROCESSING)
            (actual_data, resp) = self._check_history_node(txid)
        except kazoo.exceptions.KazooException as err:
            _LOG.exception('Zookeeper error %s', err)
        except kazoo.handlers.threading.KazooTimeoutError as err:
            _LOG.exception('Zookeeper timed out - %s', err)
        else:
            if actual_data is not None:
                actual_data = {'status': actual_data}
            return (actual_data, resp)
        return (None, None)

    def upload_batch(self, batchsize, interval):
        """Generate snapshot DB and upload to zk.
        """
        if not self.zk.exists('/history'):
            self.zk.create('/history', makepath=True,
                           acl=self.acl)
        while True:
            children = self.zk.get_children('/')
            if 'history' in children:
                children.remove('history')
            journals = [x for x in children if '_lock' not in x]
            journaltobewritten = []
            locks = [
                self.zk.Lock('/' + node + '_lock') for node in journals
            ]
            locked_nodes = []
            try:
                for (i, journal) in enumerate(journals):
                    stepkids = self._get_stepkids(journal)
                    if (locks[i].acquire(blocking=False) and stepkids):
                        nodes_to_be_written = [
                            '/'.join(['', journal, step]) for step in stepkids
                        ]
                        journaltobewritten.extend(nodes_to_be_written)
                        locked_nodes.append(journal)
                    if len(journaltobewritten) >= batchsize:
                        break
                if journaltobewritten:
                    self._create_sqlite(journaltobewritten, locked_nodes)
            except kazoo.exceptions.KazooException as err:
                _LOG.exception('Error in uploading - %s', err)
            finally:
                for lock in locks:
                    lock.release()
                self._delete_lock_nodes(locked_nodes)
            time.sleep(interval)

    def _get_stepkids(self, journal):
        try:
            stepkids = self.zk.get_children('/' + journal)
        except kazoo.exceptions.NoAuthError as err:
            _LOG.exception('Auth error for zk node %s', err)
            return []
        return stepkids

    def _delete_lock_nodes(self, lockednodes):
        for node in lockednodes:
            try:
                self.zk.delete('/' + node + '_lock')
            except kazoo.exceptions.KazooException as err:
                _LOG.exception('%s', err)

    def _create_sqlite(self, journaltobewritten, lockednodes):
        """
        Read node and create sqlite node
        """
        batchdata = []
        journalwritten = []
        for nodepath in journaltobewritten:
            try:
                data, _ = self.zk.get(nodepath)
            except kazoo.exceptions.NoAuthError as err:
                _LOG.exception('Auth error for zk node %s', err)
                continue
            olddata = zlib.decompress(data).decode()
            data_dict = json.loads(olddata)
            final_data = (
                data_dict.get('host'), data_dict.get('authuser_id'),
                data_dict.get('user_id'), data_dict.get('date'),
                data_dict.get('request_id'),
                data_dict.get('transaction_id'), data_dict.get('step'),
                data_dict.get('role'),
                data_dict.get('resourcegroup'), data_dict.get('resource'),
                data_dict.get('verb'), data_dict.get('resourcepk'),
                json.dumps(data_dict.get('payload')), data_dict.get('cm'))
            batchdata.append(final_data)
            journalwritten.append(nodepath)
        self._fold_sqlite_data(batchdata,
                               journalwritten,
                               lockednodes)

    def _fold_sqlite_data(self, batchdata,
                          journalwritten,
                          journalemptynodes):
        conn = sqlite3.connect(":memory:")
        try:
            with conn:
                conn.execute(SQLITE_CREATE)
                conn.executemany(
                    SQLITE_INSERT, batchdata)
        except sqlite3.IntegrityError:
            _LOG.exception('Error in inserting data to sqlite')
            conn.close()
            return
        fdata = '\n'.join(conn.iterdump())
        conn.close()
        childnode = '/history/sqlite-db#'
        transaction = self.zk.transaction()
        db_node = transaction.create(
            childnode, value=zlib.compress(fdata.encode()),
            acl=self.acl, sequence=True)
        _LOG.info(
            'Uploaded compressed snapshot DB: to: %s',
            db_node)
        # Delete uploaded nodes from zk.
        for oldjournal in journalwritten:
            transaction.delete(oldjournal)
        results = transaction.commit()
        if any((isinstance(e, Exception) for e in results)):
            _LOG.error('Transaction commit error - %r', results)
        self._delete_empty_nodes(journalemptynodes)

    def _delete_empty_nodes(self, journalemptynodes):
        for enode in journalemptynodes:
            try:
                self.zk.delete('/' + enode)
            except kazoo.exceptions.NotEmptyError as err:
                continue
            except kazoo.exceptions.KazooException as err:
                _LOG.exception('Error in zk delete %s', err)
                continue

    def _check_history_node(self, txid):
        for data in HISTORY_CACHE.values():
            (status, code) = self._get_history_data(data, txid)
            if code is not None:
                return (status, code)
        if self.zk.exists('/history'):
            entries = self.zk.get_children('/history')
            entries.sort(key=functools.cmp_to_key(entry_cmp), reverse=True)
            (status, code) = self._check_history_update_cache(entries, txid)
            if code is not None:
                return (status, code)
        return (None, None)

    def _get_history_data(self, sqlite_data, txid):
        conn = sqlite3.connect(":memory:")
        fdata = zlib.decompress(sqlite_data).decode()
        conn.executescript(fdata)
        conn.row_factory = sqlite3.Row
        result = conn.execute(SQLITE_SELECT, (txid, 'commit'))
        row = result.fetchone()
        if row is not None:
            actual_data = {key: row[key] for key in row.keys()}
            conn.close()
            actual_data['payload'] = json.loads(actual_data['payload'])
            return (actual_data, http.client.OK)
        result = conn.execute(SQLITE_SELECT, (txid, 'abort'))
        row = result.fetchone()
        if row is not None:
            actual_data = {key: row[key] for key in row.keys()}
            conn.close()
            actual_data['payload'] = json.loads(actual_data['payload'])
            return (actual_data, http.client.OK)
        result = conn.execute(SQLITE_SELECT, (txid, 'begin'))
        row = result.fetchone()
        if row is not None:
            actual_data = None
            conn.close()
            return (actual_data, http.client.PROCESSING)
        return (None, None)

    def _check_history_update_cache(self, entries, txid):
        _LOG.debug('number of entries %d', len(entries))
        if not entries:
            HISTORY_CACHE.clear()
            return (None, None)
        _LOG.debug('entries are %r', entries)
        if len(entries) > self.cachesize:
            cache_oldest = entries[self.cachesize - 1]
        else:
            cache_oldest = entries[-1]
        # cleanup cache first
        for key in list(HISTORY_CACHE.keys()):
            if entry_cmp(key, cache_oldest) < 0:
                del HISTORY_CACHE[key]
        # check for data and fill the cache
        status = None
        code = None
        current_cache_size = len(HISTORY_CACHE)
        for entry in entries:
            if entry in HISTORY_CACHE:
                continue
            data, _ = self.zk.get('/history/' + entry)
            if current_cache_size < self.cachesize:
                HISTORY_CACHE[entry] = data
                current_cache_size += 1
            if code is None:
                (status, code) = self._get_history_data(data, txid)
            if code is not None and current_cache_size >= self.cachesize:
                break
        if code is not None:
            return (status, code)
        return (None, None)

    def dump(self, nfspath, interval, outfile, nfsregex):
        """
        Dump sqlite node to NFS
        """
        lockfile = os.path.join(
            nfspath, self.zk.chroot.replace('/', '') + '.lock')
        while True:
            with open(lockfile, 'w') as f:
                try:
                    fcntl.lockf(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    try:
                        if self.zk.exists('/history'):
                            oldjournal = self.zk.get_children('/history')
                            oldjournal.sort(
                                key=functools.cmp_to_key(entry_cmp))
                            self._dump_sqlite_to_csv(
                                oldjournal, nfspath, outfile, nfsregex)
                    except kazoo.exceptions.SessionExpiredError:
                        _LOG.exception('Zookeeper down - session expired')
                    except kazoo.exceptions.KazooException as err:
                        _LOG.exception('Error in zk create %s', err)
                except IOError as err:
                    if err.errno == errno.EAGAIN:
                        # this is a lock fail, just skip
                        pass
                    elif err.errno == errno.EACCES:
                        # this is a lock fail, just skip
                        pass
                    else:
                        # this is another file operation exception
                        _LOG.exception(
                            'Error in acquiring lock for dump function')
            time.sleep(interval)

    def _dump_sqlite_to_csv(self, journals, nfspath, outfile, nfsregex):
        lastid = self._getlastid(nfspath, outfile, nfsregex)
        for journal in journals:
            jseqid = _get_journal_seqid(journal)
            if sequence_cmp(lastid, jseqid) < 0:
                csvfilename = outfile + '#' + jseqid + '.csv'
                csvfile = os.path.join(nfspath, csvfilename)
                try:
                    with open(csvfile, "a") as outputfile:
                        writer = csv.DictWriter(
                            outputfile,
                            fieldnames=CSV_COLUMNS)
                        writer.writeheader()
                except IOError as err:
                    _LOG.exception('Error in writing to NFS %s', err)
                    continue
                conn = sqlite3.connect(":memory:")
                try:
                    data, _ = self.zk.get('/history/' + journal)
                except kazoo.exceptions.KazooException as err:
                    _LOG.exception('Error in zk %s', err)
                    continue
                fdata = zlib.decompress(data).decode()
                conn.executescript(fdata)
                conn.row_factory = sqlite3.Row
                for row in conn.execute(SQLITE_SELECT_ALL):
                    actual_data = {key: row[key] for key in row.keys()}
                    self._convert_dict_csv(csvfile,
                                           actual_data)
                conn.close()
                lastid = jseqid
                gzipcsvfile = csvfile + '.gz'
                try:
                    with open(
                            csvfile, 'rb') as f_in, gzip.open(
                                gzipcsvfile, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                # W0703: (broad-except)
                except Exception:  # pylint: disable=W0703
                    _LOG.exception('failed to gzip %s', csvfile)
                    os.chmod(csvfile, 0o644)
                else:
                    os.chmod(gzipcsvfile, 0o644)
                    os.remove(csvfile)

    def cleanup(self, nfspath, interval, age, outfile, nfsregex):
        """
        Cleanup old sqlite node
        from zookeeper
        """
        # R1702 (too-many-nested-blocks)
        while True:  # pylint: disable=R1702
            try:
                if self.zk.exists('/history'):
                    lastid = self._getlastid(nfspath, outfile, nfsregex)
                    oldjournal = self.zk.get_children('/history')
                    for journal in oldjournal:
                        _, stat = self.zk.get('/history/' + journal)
                        if (time.time() - stat.ctime / 1000) > age:
                            jseqid = _get_journal_seqid(journal)
                            if sequence_cmp(jseqid, lastid) <= 0:
                                self.zk.delete('/history/' + journal)
                            else:
                                _LOG.info("Node:%s not dumped ", journal)
            except kazoo.exceptions.SessionExpiredError:
                _LOG.exception('Zookeeper down - session expired')
            except kazoo.exceptions.NoNodeError as err:
                _LOG.exception('Node already got deleted')
            except kazoo.exceptions.KazooException as err:
                _LOG.exception('Error in zk delete %s', err)
            time.sleep(interval)

    def _getlastid(self, nfspath, outfile, nfsregex):
        lastid = None
        pattern = '{0}/{1}*'.format(nfspath, outfile)
        files = glob.glob(pattern)
        for jfile in files:
            result = nfsregex.match(jfile)
            if result:
                if lastid is None:
                    lastid = result.groups()[0]
                if int(lastid) < int(result.groups()[0]):
                    lastid = result.groups()[0]
        return lastid

    def _convert_dict_csv(self, csvfile, dict_data):
        # Converting journal data to splunk specific data
        dict_data['role'] = dict_data['as_role']
        dict_data['pk'] = dict_data['resourcepk']
        del dict_data['as_role']
        del dict_data['resourcepk']
        del dict_data['resourcegroup']
        try:
            with open(csvfile, "a") as outfile:
                writer = csv.DictWriter(outfile, fieldnames=CSV_COLUMNS)
                writer.writerow(dict_data)
        except IOError as err:
            _LOG.exception('Error in writing to NFS %s', err)


def _get_journal_seqid(journal):
    result = SQLITE_NODE_REGEX.match(journal)
    if result:
        return result.groups()[0]
    else:
        return None


def entry_cmp(sqlite_file1, sqlite_file2):
    """
    Compare two sqlite file entries
    in zookeeper to know the ordering
    """
    seq_id1 = _get_journal_seqid(sqlite_file1)
    seq_id2 = _get_journal_seqid(sqlite_file2)
    return sequence_cmp(seq_id1, seq_id2)


def sequence_cmp(seqid1, seqid2):
    """
    Serial number arithmetic
    """
    if seqid1 == seqid2:
        return 0
    if seqid1 is None:
        return -1
    if seqid2 is None:
        return 1
    seq_id1 = int(seqid1)
    seq_id2 = int(seqid2)
    if (
            (
                seq_id1 < seq_id2 and (
                    seq_id2 - seq_id1) < 2**(SERIAL_BITS - 1)
            ) or
            (
                seq_id1 > seq_id2 and (
                    seq_id1 - seq_id2) > 2**(SERIAL_BITS - 1)
            )
    ):
        return -1
    else:
        return 1

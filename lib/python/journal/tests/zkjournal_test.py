"""
Unit test for zookeeper journal write
"""

import unittest
import json
import zlib
import kazoo
import mock  # pylint: disable=E0401

from journal.zkjournal import ZookeeperJournal, entry_cmp
from journal.zk.client.zookeeper import ZkClient


class ZkjournalTestCase(unittest.TestCase):
    """Mock test for zookeeper journal"""

    @mock.patch('kazoo.client.KazooClient.start', mock.Mock())
    @mock.patch('kazoo.client.KazooClient.create', mock.Mock())
    @mock.patch(
        'kazoo.client.KazooClient.exists', mock.Mock(return_value=True))
    @mock.patch('journal.zk.utils.connect',
                mock.Mock(return_value=ZkClient()))
    @mock.patch('kazoo.client.KazooClient.connected',
                mock.PropertyMock(return_value=True))
    def test_write(self):
        """ Test for writing to zookeeper"""
        kwargs = dict()
        zkj = ZookeeperJournal('zookeeper://dev#foobar',
                               kwargs, 'adminuser', 50)
        zkj.journal_zk_start()
        msg = {'user_id': 'user1', 'role': None,
               'request_id': 'BAD268C6-AB14-11E6-A7C1-98638C7A8FAA',
               'transaction_id': 'BAD268C6-AB14-11E6-A7C1-98638C7A8FAA',
               'step': 'commit', 'resource': 'phonebook', 'cm': None}
        json_msg = json.dumps(msg)
        compressed_msg = zlib.compress(json_msg.encode())
        zkj.write(msg['request_id'], 'commit', msg)
        kazoo.client.KazooClient.create.assert_called_with(
            '/BAD268C6-AB14-11E6-A7C1-98638C7A8FAA/commit',
            value=compressed_msg, makepath=True, acl=zkj.acl)

    def test_sequence_cmp(self):
        """ Mock test serial number arithmetic logic"""
        sqlite_file1 = 'sqlite-db#0000000010'
        sqlite_file2 = 'sqlite-db#-2147473647'
        result = entry_cmp(sqlite_file1, sqlite_file2)
        self.assertEqual(result, 1)
        sqlite_file2 = 'sqlite-db#0000001000'
        result = entry_cmp(sqlite_file1, sqlite_file2)
        self.assertEqual(result, -1)
        sqlite_file1 = 'sqlite-db#2147473647'
        sqlite_file2 = 'sqlite-db#-2147473647'
        result = entry_cmp(sqlite_file1, sqlite_file2)
        self.assertEqual(result, -1)


if __name__ == '__main__':
    unittest.main()

"""
Script to fold zk nodes to
sqlite db
"""
import sys
import yaml
from journal import zkjournal


def main(args):
    """
    Command line journal
    """
    primary_journal = None
    kwargs = dict()
    if args.cfg:
        with open(args.cfg) as config:
            jconf = yaml.load(config)
            if 'primary' in jconf:
                primary_journal = jconf['primary']
                jconf.pop('primary')
            kwargs = jconf
    if args.primary:
        primary_journal = args.primary
    if primary_journal is None:
        sys.exit("Missing primary journal")
    (jmodule, jval) = primary_journal.split('://')
    str(jmodule).lower()
    if 'zookeeper' not in jmodule:
        sys.exit("Wrong zookeeper information")
    zkj = zkjournal.ZookeeperJournal(primary_journal, kwargs, args.adminuser)
    zkj.journal_zk_start()
    if zkj.zk.connected:
        zkj.upload_batch(
            args.batchsize,
            args.interval)
    sys.exit()

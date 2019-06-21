"""
Journal webserver startup
NFS resync process
"""

import sys
import logging
import argparse

from journal import journal_server_main
from journal import resync_nfs_main
from journal import journal_cli_main
from journal import journal_zk_sqlite_main
from journal import journal_zk_cleanup_main
from journal import journal_zk_dump_main

FORMAT = '[%(asctime)s] [%(filename)s] [%(process)d] '\
         '[%(levelname)s]: %(message)s'


def resync_nfs():
    """
    Journal resync with nfs entry function
    """
    logging.basicConfig(format=FORMAT,
                        level=logging.INFO,
                        datefmt='%m/%d/%Y %I:%M:%S %p')
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--cfg',
                        help='Journal config file')
    parser.add_argument('-p', '--primary',
                        help='Primary journal')
    parser.add_argument('-s', '--secondary',
                        help='Secondary journal')
    parser.add_argument('--adminuser',
                        help='Admin user which has rw/delete access')
    args = parser.parse_args()
    if args.cfg or args.primary and args.secondary:
        resync_nfs_main.main(args.adminuser,
                             args.cfg,
                             args.primary,
                             args.secondary)
    else:
        sys.exit("Journal config missing: type --help to see options")


def journal_startup():
    """
    Journal web server start up entry function
    """
    logging.basicConfig(format=FORMAT,
                        level=logging.INFO,
                        datefmt='%m/%d/%Y %I:%M:%S %p')
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--cfg',
                        help='Journal config file')
    parser.add_argument('-u', '--unixsocket', required=True,
                        help='Socket for webserver to listen on')
    parser.add_argument('-p', '--primary',
                        help='Primary journal')
    parser.add_argument('-s', '--secondary',
                        help='Secondary journal')
    parser.add_argument('-i', '--historycache',
                        default=50, type=int,
                        help='size of history cache')
    parser.add_argument('--adminuser',
                        help='Admin user which has rw/delete access')
    args = parser.parse_args()
    journal_server_main.main(args)


def journal_cli():
    """
    Journal cli to write to journal
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--primary',
                        help='Primary journal')
    parser.add_argument('-s', '--secondary',
                        help='Secondary journal')
    parser.add_argument('--adminuser',
                        help='Admin user which has rw/delete access')
    parser.add_argument('-c', '--cfg',
                        help='Journal config file')
    args = parser.parse_args()
    journal_cli_main.main(args)


def journal_zk_sqlite():
    """
    Zk to journal sqlite folding
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--cfg',
                        help='Journal config file')
    parser.add_argument('-p', '--primary',
                        help='Zookeeper journal')
    parser.add_argument('-b', '--batchsize',
                        default=2000, type=int,
                        help='Batch size')
    parser.add_argument('-i', '--interval',
                        default=300, type=int,
                        help='Interval in seconds')
    parser.add_argument('--adminuser',
                        help='Admin user which has rw/delete access')
    args = parser.parse_args()
    journal_zk_sqlite_main.main(args)


def journal_zk_dump():
    """
    Zk to NFS dump for vault/splunk
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--cfg',
                        help='Journal config file')
    parser.add_argument('-p', '--primary',
                        help='Zookeeper journal')
    parser.add_argument('-n', '--nfspath',
                        required=True,
                        help='NFS path')
    parser.add_argument('-i', '--interval',
                        default=300, type=int,
                        help='Interval in seconds')
    parser.add_argument('-r', '--nfsregex', required=True,
                        help='Pattern of files in nfs')
    parser.add_argument('-o', '--outfile', required=True,
                        help='dump output file name')
    args = parser.parse_args()
    journal_zk_dump_main.main(args)


def journal_zk_cleanup():
    """
    Zk history sqlite node cleanup
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--cfg',
                        help='Journal config file')
    parser.add_argument('-p', '--primary',
                        help='Zookeeper journal')
    parser.add_argument('-n', '--nfspath',
                        help='NFS path')
    parser.add_argument('-i', '--interval',
                        default=900, type=int,
                        help='Interval in seconds')
    parser.add_argument('-a', '--age',
                        default=3600, type=int,
                        help='age in seconds')
    parser.add_argument('-r', '--nfsregex', required=True,
                        help='Pattern of files in nfs')
    parser.add_argument('-o', '--outfile', required=True,
                        help='dump output file name')
    args = parser.parse_args()
    journal_zk_cleanup_main.main(args)

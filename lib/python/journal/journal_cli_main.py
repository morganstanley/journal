"""
Journal cli
"""
import json
import struct
import sys
import yaml
from journal import mjournal


def journal_init(in_args):
    """
    Initialize journal locally
    """
    jconfig = dict()
    kwargs = dict()
    if in_args.cfg:
        with open(in_args.cfg) as config:
            jconf = yaml.load(config)
            if 'primary' in jconf:
                jconfig['primary'] = jconf['primary']
                jconf.pop('primary')
            if 'secondary' in jconf:
                jconfig['secondary'] = jconf['secondary']
                jconf.pop('secondary')
            kwargs = jconf
    if in_args.primary:
        jconfig['primary'] = in_args.primary
    if in_args.secondary:
        jconfig['secondary'] = in_args.secondary
    if 'primary' not in jconfig and 'secondary' not in jconfig:
        sys.exit("Missing primary and secondary journal")
    jconfig['adminuser'] = in_args.adminuser
    kwargs = dict()
    return mjournal.Journal(jconfig, kwargs)


def write(journal_obj, msg):
    """
    Write to journal
    """
    txid = msg['request_id']
    step = msg['step']
    rc = journal_obj.write(txid, step, msg)
    return rc


def main(args):
    """
    Command line journal
    """
    journalobj = journal_init(args)
    # read input from stdin
    length = sys.stdin.buffer.read(4)
    msglen = struct.unpack('!I', length)
    line = sys.stdin.read(msglen[0])
    msg = json.loads(line)
    rc = write(journalobj, msg)
    sys.exit(rc)

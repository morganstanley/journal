# Journal
## Description: Distributed ledger / Distributed State store based on Zookeeper and NFS.

![Lifecycle Archived](https://badgen.net/badge/Lifecycle/Archived/grey)

# Summary
#########
# This project can be used with asynchronous API server or automation tools to keep track of long running requests.
# A highly structured and compressed data with various stages of the request like start, authorization, commit and abort  is stored.
# We offer support of Zookeeper as primary and NFS as secondary for storing state data.
# After certain interval(tunable), these data can be pushed to an external storage.

#####################################################################################
#Instruction to run journal server process:
#####################################################################################
usage: journal_webserver.py [-h] [-c CFG] -u UNIXSOCKET [-p PRIMARY]
                            [-s SECONDARY]
journal_webserver.py: error: the following arguments are required: -u/--unixsocket

unixsocket - Gunicorn webserver listen on this socket for incoming http requests

cfg - Config specifying primary and secondary journal.
Example format of config:
Example 1:
journal:
  primary: zookeeper://zkurl
  secondary: nfs:///tmp/nfspath

########################################################################################
#Client code:
########################################################################################
The journal server supports json payload.
Example of Http requests format - unix socket(/tmp/journal.sock), sub directory (14l62dae-2e2a-448e-94f9-te43d86d4cd0), filename - (begin)
http+unix://%2Ftmp%2F{journal.sock/14l62dae-2e2a-448e-94f9-te43d86d4cd0/begin

Example python client program
---------------------------
#! /usr/bin/env python

import requests_unixsocket
import requests
import json
requests_unixsocket.monkeypatch()

if __name__ == '__main__':
    msg = {
        'user_id': 'user1',
        'resourcepk': None,
        'request_id': 'ac513125-0469-408d-b332-b8a0383870f9',
        'host': 'host1',
        'payload': None,
        'resourcegroup': 'cookbook',
        'transaction_id': 'c4f62dae-2c3a-417e-94f9-db43d86d4cd0',
        'cm': None,
        'step': 'begin',
        'date': '2017-6-13 16:47:55',
        'resource': 'cookbook/todo',
        'role': None,
        'authuser_id': 'user1',
        'verb': 'get'
    }
    url = 'http+unix://%2Ftmp%2Fjournal.sock/{0}/{1}'.format('ac513125-0469-408d-b332-b8a0383870f9', 'begin')
    r  = requests.post(url, json=msg, headers={"Accept": 'application/json', 'Content-Type': 'application/json'})
    print(r.status_code)
    print(r.content)

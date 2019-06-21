"""
Module containing route for zookeeper journal
"""

import http.client
import json
import logging
from flask import request, current_app, make_response
from journal.main import MAIN

# W0611: Unused import
# This module import is needed for journal_obj.write
from journal import mjournal  # pylint: disable=W0611
from journal.main import errors

_LOG = logging.getLogger(__name__)


@MAIN.route('/<string:txid>/<string:step>', methods=['POST'])
def journalview(txid, step):
    """
    Handler for journal write
    """

    payload = request.get_json()
    journal_obj = current_app.config['journal']
    rc = journal_obj.write(txid, step, payload)
    if rc == 0:
        return('', http.client.CREATED)
    else:
        _LOG.critical('Unsaved journal entry %s:%s', txid, step)
        raise errors.APIError(
            '{0} -- {1}##{2}'.format(
                'Unsaved Journal entry',
                txid,
                step),
            status_code=http.client.INTERNAL_SERVER_ERROR)


@MAIN.route('/status/<string:txid>', methods=['GET'])
def journalstatus(txid):
    """
    Handler for journal status
    """

    journal_obj = current_app.config['journal']
    (resp, status_code) = journal_obj.status(txid)
    if resp is not None:
        resp = json.dumps(resp)
    output = make_response(resp, status_code)
    output.headers['Content-Type'] = 'application/json'
    return output

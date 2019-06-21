"""
Error handler for zookeeper journal
web application
"""
import http.client
from flask import jsonify
from . import MAIN


class APIError(Exception):
    """
    Exception sub class for
    application related errors
    """

    status_code = http.client.INTERNAL_SERVER_ERROR

    def __init__(self, message, status_code=None, payload=None):
        super(APIError, self).__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        """
        constructing dictionary
        for error message
        """

        rv = dict(self.payload or ())
        rv['message'] = self.message
        return rv


@MAIN.app_errorhandler(APIError)
def handle_api_error(error):
    """
    Error handler for application errors
    """

    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


__all__ = ['APIError']

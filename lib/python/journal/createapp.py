"""
Module responsible for creating
flask app using blueprint
"""

import flask
from flask import current_app, request, abort
from journal import mjournal

from journal.main import MAIN as main_blueprint


def create_app(journal_config, kwargs):
    """
    Function which returns a flask app
    """

    app = flask.Flask(__name__)
    with app.app_context():
        app.config['journal_config'] = journal_config
        app.config['extra_args'] = kwargs

    # W0612: Unused variable 'before_request'.
    # This function is internally called by flask app
    @app.before_request
    def only_json():  # pylint: disable=W0612
        """
        Abort the request if its not json type
        """
        if not request.is_json:
            abort(400)

    # W0612: Unused variable 'before_first_request'.
    # This function is internally called by flask app
    @app.before_first_request
    def before_first_request():  # pylint: disable=W0612
        """
        Create primary and secondary journal objects
        """
        journalobj = mjournal.Journal(current_app.config['journal_config'],
                                      current_app.config['extra_args'])
        current_app.config['journal'] = journalobj
    app.register_blueprint(main_blueprint)
    return app

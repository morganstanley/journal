"""
Create Blueprint for flask
"""

from flask import Blueprint

MAIN = Blueprint('main', __name__)

# E402: module level import not at top of file
# C0413: import should be placed at the top of the module
# views, errors module should be loaded only after creating blueprint

from . import views, errors  # noqa: E402, pylint: disable=C0413

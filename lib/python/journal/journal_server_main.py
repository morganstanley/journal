"""
Main module to start
journal web server
"""
import sys
import os
import logging
import errno

import yaml
from gunicorn.app.base import Application
from journal import createapp

_LOG = logging.getLogger(__name__)
FORMAT = '[%(asctime)s] [%(filename)s] [%(process)d] '\
         '[%(levelname)s]: %(message)s'


class FlaskApp(Application):
    """
    Load flask application by gunicorn web server
    """
    def __init__(self, options, app):
        self.options = options
        self.app = app
        super(FlaskApp, self).__init__()

    def init(self, parser, opts, args):
        """
        Passing options to gunicorn web server
        """
        return {'bind': self.options['bind'],
                'timeout': self.options['timeout']}

    def load(self):
        """
        Load flask application
        """

        return self.app


def main(args):
    """
    Journal web server start up entry function
    """
    jconfig = dict()

    kwargs = dict()
    if args.cfg:
        with open(args.cfg) as config:
            jconf = yaml.load(config)
            if 'primary' in jconf:
                jconfig['primary'] = jconf['primary']
                jconf.pop('primary')
            if 'secondary' in jconf:
                jconfig['secondary'] = jconf['secondary']
                jconf.pop('secondary')
            kwargs = jconf
    journal_socket = 'unix:{0}'.format(args.unixsocket)

    if args.primary:
        jconfig['primary'] = args.primary
    if args.secondary:
        jconfig['secondary'] = args.secondary
    jconfig['cachesize'] = args.historycache
    jconfig['adminuser'] = args.adminuser
    if 'primary' not in jconfig and 'secondary' not in jconfig:
        sys.exit("Missing primary and secondary journal")
    try:
        os.unlink(journal_socket)
    except OSError as err:
        if err.errno == errno.ENOENT:
            # File did not exist - ok
            pass
        else:
            # Real error
            raise
    app = createapp.create_app(jconfig, kwargs)
    sys.argv = sys.argv[:1]
    opt = {'bind': journal_socket, 'timeout': 60}
    FlaskApp(opt, app).run()

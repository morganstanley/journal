"""
Zk client base module
"""
import pkg_resources


def get_scheme_module(scheme):
    """Import and return the Zookeeper client's scheme module.
    """
    plugin = None
    for ep in pkg_resources.iter_entry_points(
            group='zookeeper_scheme', name=scheme):
        plugin = ep.load()
    if plugin is None:
        raise NotImplementedError('Unknown Zookeeper scheme %r' % scheme)
    return plugin

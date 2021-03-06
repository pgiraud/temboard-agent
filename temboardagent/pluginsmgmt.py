import logging
import os
import imp
import time

from temboardagent.spc import connector


logger = logging.getLogger(__name__)


def load_plugins_configurations(config):
    """
    Intend to load plugins and run their configuration() function.
    Plugins are defined as a module located in plugins/ directory. The list
    of plugins to load is set into temboard section of the configuration file:
        [temboard]
        plugins = [ "plugin1", "plugin2" ]
    """

    # Compat with legacy plugins
    config.configfile = config.temboard.configfile
    # Get this module's path.
    path = os.path.dirname(__file__)
    ret = dict()
    # PostgreSQL version
    pg_version = 0

    while pg_version == 0:
        try:
            conn = connector(
                host=config.postgresql['host'],
                port=config.postgresql['port'],
                user=config.postgresql['user'],
                password=config.postgresql['password'],
                database=config.postgresql['dbname']
            )
            """ Trying to get PostgreSQL version number. """
            conn.connect()
            pg_version = conn.get_pg_version()
            conn.close()
        except Exception as e:
            logger.exception(str(e))
            logger.error("Not able to get PostgreSQL version number.")
            try:
                conn.close()
            except Exception:
                pass

        # If we reach this point, PostgreSQL is not available, so we
        # wait 5 seconds and try again
        if pg_version == 0:
            time.sleep(5)

    # Loop through each plugin listed in the configuration file.
    for plugin_name in config.temboard['plugins']:
        logger.info("Loading legacy plugin '%s'." % (plugin_name,))
        fp_s = None
        try:
            # Loading compat.py file
            fp_s, path_s, desc_s = imp.find_module(
                                        'compat',
                                        [path+'/plugins/'+plugin_name])
            module_compat = imp.load_module('compat',
                                            fp_s,
                                            path_s,
                                            desc_s)
            # Check modules's PG_MIN_VERSION
            try:
                if (module_compat.PG_MIN_VERSION > pg_version):
                    # Version not supported
                    logger.error("PostgreSQL version (%s) is not supported "
                                 "(min:%s)." % (pg_version,
                                                module_compat.PG_MIN_VERSION))
                    logger.info("Failed.")
                    continue
            except ValueError as e:
                # PG_MIN_VERSION not set
                pass
        except Exception as e:
            if fp_s:
                fp_s.close()
            logger.info("Not able to load the compatibility file: compat.py.")
        logger.info("Done.")
        fp = None
        try:
            # Locate and load the module with imp.
            fp, pathname, description = imp.find_module(plugin_name,
                                                        [path+'/plugins'])
            module = imp.load_module(plugin_name, fp, pathname, description)
            # Try to run module's configuration() function.
            logger.info(
                "Loading legacy plugin '%s' configuration." % (plugin_name,))
            plugin_configuration = getattr(module, 'configuration', None)
            ret.update({module.__name__: plugin_configuration})
            assert plugin_configuration, "undefined configuration"
            ret[plugin_name] = plugin_configuration(config)
            logger.info("Done.")
        except AssertionError as e:
            logger.warn("No configuration: %s", e)
        except Exception as e:
            if fp:
                fp.close()
            logger.exception(str(e))
            logger.info("Failed.")

    return ret

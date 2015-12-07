from collections import namedtuple
from bisect import insort
import logging
import json, yaml
import importlib
import inspect

logger = logging.getLogger(__name__)

config = None
handlers = {}
lockagent = None

events = [
    "all",
# agent sequence
    "all_agent",
    "pre_snapshot",
    "start_snapshot",
    "create_snapshot",
    "save_snapshot",
    "delete_snapshot",
    "end_snapshot",
    "post_snapshot",
# restore sequence
    "all_restore",
    "pre_restore",
    "start_restore",
    "do_restore",
    "end_restore",
    "post_restore",
]


def local_config(args = {}):
    try:
        with open('/etc/znappy/config.yaml') as f:
            config = yaml.load(f)
            args['--cluster'] = config.get('cluster')
            args['--log-level'] = str.upper(config.get('log-level', 'WARN'))
            args['--host']    = config.get('consul', {}).get('hostname', 'localhost')
            args['--port']    = config.get('consul', {}).get('port', 8500)
    except Exception, e:
        logger.warn('Failed to load local configuration!')
        exit(1)

    return args


def load_drivers(config, snapshot):
    for pkgname in config['drivers'].keys():
        try:
            pkg = importlib.import_module(pkgname)

            pkg.load_handlers(
                config['drivers'].get(pkgname, {}),
                snapshot,
                register_handler
            )
        except ImportError, e:
            logger.info("Failed to load package: {}.. skipping".format(e.message))
        except Exception, e:
            logger.fatal("Failed to load package: {}.. exiting".format(e.message))
            raise e
            exit(1)


class PrioritizedHandler(namedtuple('PrioritizedHandler', ('priority', 'driver', 'callback'))):
    def __lt__(self, other):
        return self.priority < other.priority


def register_handler(event, handler, priority=0):
    global handlers

    # Get the module name of whomever is calling me
    caller = inspect.getmodule(inspect.stack()[1][0]).__name__

    logger.debug("caller: {0}, event: {1}, priority: {2}".format(caller, event, priority))

    # refactor the handler to use priority and driver name
    handler = PrioritizedHandler(priority, caller, handler)

    event_handlers = handlers.setdefault(event, [])
    insort(event_handlers, handler)


def execute_event(events, driver = None, *args, **kwargs):
    global handlers, logger

    matching_handlers = []
    
    for e in list(events) + ["all"]:
        matching_handlers = matching_handlers + handlers.get(e, [])

    matching_handlers = sorted(matching_handlers)

    if driver is not None:
        matching_handlers = filter(lambda h: h.driver == driver, matching_handlers)

    logger.debug("[{2}] {0} => {1}".format(list(events), matching_handlers, driver))

    for handle in matching_handlers:
        # probabably want to do this with exceptions
        status, message = handle.callback(*args, **kwargs)
        
        logger.debug("{0} => {1}".format(status, message))
        if not status:
            logger.critical('callback failed, stopping')
            return status, message

    return True, 'All callbacks returned successfully'

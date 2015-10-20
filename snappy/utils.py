from collections import namedtuple
from bisect import insort
import logging
import yaml

logger = logging.getLogger(__name__)

config = None
handlers = {}
keystore = None
lockagent = None

events = [
    "all",
# agent sequence
    "all_agent",
    "pre_snapshot",
    "start_snapshot",
    "create_snapshot",
    "save_snapshot",
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

def load_config(filename):
    global config

    with open(filename) as handle:
        config = yaml.load(handle)


class PrioritizedHandler(namedtuple('PrioritizedHandler', ('priority', 'callback'))):
    def __lt__(self, other):
        return self.priority < other.priority


def register_handler(event, handler, priority=0):
    global handlers
    handler = PrioritizedHandler(priority, handler)

    event_handlers = handlers.setdefault(event, [])
    insort(event_handlers, handler)


def execute_event(*events):
    global handlers, logger

    matching_handlers = []
    
    for e in list(events) + ["all"]:
        matching_handlers = matching_handlers + handlers.get(e, [])

    matching_handlers = sorted(matching_handlers)

    logger.debug("{0} => {1}".format(list(events), matching_handlers))

    for handle in matching_handlers:
        # probabably want to do this with exceptions
        status, message = handle.callback()
        
        if not status:
            logger.critical('callback failed, stopping')
            return status, message

    return True, 'All callbacks returned successfully'

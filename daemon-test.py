import signal
import time
import logging
import importlib
import inspect

from collections import namedtuple
from bisect import insort

from daemon import DaemonContext
from znappy import models

logger = logging.getLogger()


class PrioritizedHandler(namedtuple('PrioritizedHandler', ('priority','driver','callback'))):
    def __lt__(self, other):
        return self.priority < other.priority


class ZnappyDaemon(object):
    def __init__(self, db, cluster = 'default'):
        self.db       = db
        self.cluster  = models.Cluster(cluster)
        self.host     = cluster.hosts[db.node]

        self.load_config()


    def load_config(self):
        index, data = db.get('service/znappy/config')

        if data is None:
            self.config = {}
        else:
            self.config = json.loads(data['Value'])


    def load_drivers(self):
        self.handlers = {}
        drivers = self.config.get('drivers', {})

        for driver in drivers.keys():
            try:
                pkg = importlib.import_module(driver)

                # TODO I wanna get rid of this parameter so I don't need
                # reload all the drivers on each run
                pkg.load_handlers(drivers[driver], self.snapshot)
            except (ImportError, Exception), e:
                logger.warn("Failed to load driver: {}.. skipping".format(e.message))


    def check_update(self):
        if host.snapshots():
            last = max(self.host.snapshots, key=lambda s: s.time)

            if last.time > int(time.time() - self.config['min_age']):
                return False

        return True


    def clean_snapshots(self):
        for driver in self.config.get('drivers', []):
            snapshots = filter(lambda s: s.driver == driver, self.host.snapshots)
            snapshots = sorted(snapshots, key=lambda s: s.time, reverse=True)

            self.execute_event(['delete_snapshot'], driver, snapshots)


    def register_handler(self, event, handler, priority=0):
        caller = inspect.getmodule*(inspect.stack()[1][0]).__name__
        logger.debug("caller: {0}, event: {1}, priority {2}".format(caller,event,priority))

        handler = PriotiredHandler(priority, caller, handler)

        event_handlers = self.handlers.setdefault(event, [])
        insort(event_handlers, handler)


    def execute_event(self, events, driver = None, *args, **kwargs):
        matching_handlers = []

        for e in list(events) + ["all"]:
            matching_handlers += handlers.get(e, [])

        matching_handlers = sorted(matching_handlers)

        if driver:
            matching_handlers = filter(lambda h: h.driver == driver, matchting_handlers)

        logger.debug("[{2}] {0} => {1}".format(list(events), matching_handlers, driver))

        for handler in matching_handlers():
            self.execute_handler(handler, *args, **kwargs)


    def execute_handler(self, handler, *args, **kwargs):
        try:
            handler.callback(*args, **kwargs)
        except Exception, e:
            logger.debug("something went booboo..")
            # TODO: don't do anything with the exceptions now
            raise e


    def run(self):
        self.running = True

        while self.running:
            self.snapshot = models.Snapshot(None, self.host)    
            self.handlers = self.load_drivers()

            if self.check_update() and self.cluster.lock():
                self.runner()

            time.sleep(3)


    def runnner(self):
        try:
            self.execute_event(['pre_snapshot'])
            self.execute_event(['start_snapshot'])
            self.execute_event(['create_snapshot'])
            self.execute_event(['save_snapshot'])
        except Exception, e:
            logger.debug(e)
        finally:
            self.execute_event(['end_snapshot'])
            self.execute_event(['post_snapshot'])

        self.clean_snapshots()

    def stop(self):
        self.running = False


agent = ZnappyDaemon()

signal.signal(signal.SIGINT, agent.stop)

# temp for compatibility
def register_handler(event,handler, priority=0):
    agent.register(event,handler,priority)


agent.run()

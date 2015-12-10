from bisect import insort
from collections import namedtuple
from znappy import models

import importlib
import inspect
import json
import logging
import time

__all__ = ['Znappy', 'ZnappyEventException']


logger = logging.getLogger(__name__)


class PrioritizedHandler(namedtuple('PrioritizedHandler', ('priority','driver','callback'))):
    def __lt__(self, other):
        return self.priority < other.priority


class ZnappyEventException(Exception):
    def __init__(self, code = 0, message = '', *args):
        super.__init__(self, *args)
        self.code= code
        self.message = message


class Znappy(object):
    def __init__(self, db, cluster):
        self.db = db
        self.clustername = cluster

        self.load_cluster()
        self.load_config()

    def load_cluster(self):
        self.cluster = models.Cluster(self.clustername)

        if not self.db.node in self.cluster.hosts:
            self.host = models.Host(self.cluster, db.node)
            self.host.save()
        else:
            self.host = self.cluster.hosts[self.db.node]

    def load_config(self):
        self.config = self.cluster.config

    def load_drivers(self):
        self.handlers = {}
        drivers = self.config.get('drivers', {})

        for driver in drivers.keys():
            try:
                pkg = importlib.import_module(driver)

                # TODO I wanna get rid of this parameter so I don't need
                # reload all the drivers on each run
                pkg.load_handlers(drivers[driver], self.snapshot, register=self.register)
            except (ImportError, Exception), e:
                raise e
                logger.warn("Failed to load driver: {}.. skipping".format(e.message))

    def check_update(self):
        snapshots = self.host.snapshots.values()
        if snapshots:
            last = max(snapshots, key=lambda s: s.time)

            if last.time > int(time.time() - self.config.get('snapshot', {'min_age': 3600})['min_age']):
                logger.debug('last snapshot is ok')
                return False

        return True

    def clean_snapshots(self):
        drivers   = self.config.get('drivers', [])
        snapshots = self.host.snapshots.values()
        logger.debug('cleaning old snapshots')
        logger.debug(drivers)

        for driver in drivers:
            driver_snapshots = filter(lambda s: s.driver == driver, snapshots)
            logger.debug(driver_snapshots)
            driver_snapshots = sorted(driver_snapshots, key=lambda s: s.time, reverse=True)
            logger.debug(driver_snapshots)

            self.execute_event(['delete_snapshot'], driver, driver_snapshots)

    def register(self, event, handler, priority=0):
        caller = inspect.getmodule(inspect.stack()[1][0]).__name__
        logger.debug("caller: {0}, event: {1}, priority {2}, handler={3}".format(caller,event,priority, handler))

        handler = PrioritizedHandler(priority, caller, handler)

        event_handlers = self.handlers.setdefault(event, [])
        insort(event_handlers, handler)

    def execute_event(self, events, driver = None, *args, **kwargs):
        matching_handlers = []

        for e in list(events) + ["all"]:
            matching_handlers += self.handlers.get(e, [])

        matching_handlers = sorted(matching_handlers)

        if driver:
            matching_handlers = filter(lambda h: h.driver == driver, matching_handlers)

        logger.debug("[{2}] {0} => {1}".format(list(events), matching_handlers, driver))

        for handler in matching_handlers:
            result, message = self.execute_handler(handler, *args, **kwargs)

            if not result:
                logger.critical("Failed to execute handler: handlers={0}, message={1}".format(handler, message))
                raise ZnappyEventException(code=result, message=message)

    def execute_handler(self, handler, *args, **kwargs):
        try:
            return handler.callback(*args, **kwargs)
        except Exception, e:
            logger.warn(e.message)
            return False, ''

    def daemon(self):
        self.running = True

        while self.running:
            self.load_cluster()
            self.run()
            self.db.ping()

            time.sleep(10)

    def run(self, force = False):
        if (self.check_update() and self.cluster.lock()) or force:
            self.snapshot = models.Snapshot(self.host)
            self.load_drivers()
            try:
                self.execute_event(['pre_snapshot'])
                self.execute_event(['start_snapshot'])
                self.execute_event(['create_snapshot'])
                self.execute_event(['save_snapshot'])
            except Exception, e:
                logger.warn(e)
            finally:
                self.execute_event(['end_snapshot'])
                self.execute_event(['post_snapshot'])
                self.cluster.release()

            self.clean_snapshots()

    def monitor(self, *args, **kwargs):
        self.snapshot = models.Snapshot(self.host)
        self.load_drivers()

        try:
            self.execute_event(['monitor'], *args, **kwargs)
        except ZnappyEventException, e:
            return e.code, e.message
#        except Exception:
#            return 3, "UNKNOWN: Failed to execute events"

        return 0, "OK: All checks are green!"

    def stop(self, sig, frame):
        self.running = False

        return True

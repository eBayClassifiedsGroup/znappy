import json
import logging

__all__ = ['db','Cluster','Host','Snapshot']

logger = logging.getLogger(__name__)
db     = None


class Model(object):
    def __repr__(self):
        return "{0}({1})".format(self.__class__.__name__, self.name)


class Cluster(Model):
    def __init__(self, name = 'default'):
        self.name  = name
        self.hosts = self._load_hosts()


    def _load_hosts(self):
        keys = map(
            lambda k: k.split('/',4)[3],
            db.get("{}/".format(self.path()), keys=True)[1] or []
        )

        keys = set(filter(
            lambda n: not(len(n) == 0 or n[0] == '.'),
            keys
        ))

        return {k: Host(k, self) for k in keys}


    def save(self):
        db.put(self.path(), None)

        for host in self.hosts:
            host.save()


    def lock(self):
        # TODO Test if we can implement a wait_for_lock
        return db.acquire("{}/.lock".format(self.path()))


    def release(self):
        return db.release("{}/.lock".format(self.path()))


    def path(self):
        return "{0}/{1}".format('service/znappy', self.name)


class Host(object):
    def __init__(self, name = None, cluster = None):
        self.name      = name or db.node
        self.cluster   = cluster
        self.snapshots = self._load_snapshots()


    def __repr__(self):
        return "{0}({1})".format(self.__class__.__name__, self.name)


    def _load_snapshots(self):
        keys = set(filter(
            lambda n: n != "",
            map(
                lambda k: k.split('/',5)[5],
                db.get("{0}/snapshots/".format(self.path()), keys=True)[1] or []
        )))

        return map(lambda v: Snapshot(v, host=self), keys)


    def save(self):
        db.put(self.path(), None)

        # add it to the cluster, to prevent reloading
        self.cluster.hosts[self.name] = self

        for snapshot in self.snapshots:
            snapshot.save()


    def path(self):
        if self.cluster is None:
            raise KeyError('cluster not set')

        return "{0}/{1}".format(self.cluster.path(), self.name)


class Snapshot(object):
    def __init__(self, name, host = None):
        self.name    = name
        self.host    = host

        if self.name is not None:
            self.driver, self.target, self.time = self._load()


    def __repr__(self):
        return "{0}({1})".format(self.__class__.__name__, self.name)


    def _load(self):
        data = json.loads(db.get(self.path())[1]['Value'] or "{}")

        return map(data.get, ['driver', 'target', 'time'])


    def save(self):
        #update host
        return db.put(self.path(), json.dumps({
            'name':   self.name,
            'driver': self.driver,
            'target': self.target,
            'time':   self.time,
        }))


    def delete(self):
        return db.delete(self.path())


    def path(self):
        if self.host is None:
            raise KeyError('host not set')

        return "{0}/snapshots/{1}".format(self.host.path(), self.name)

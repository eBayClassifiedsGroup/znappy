import json
import logging

__all__ = ['db','Cluster','Host','Snapshot']

logger = logging.getLogger(__name__)
db     = None


class Model(object):
    def __repr__(self):
        return "{0}({1})".format(self.__class__.__name__, self.name)


    def _load_children(self, path_str, obj):
        path = path_str.format(self.path())
        keys = filter(None, set(map(
            lambda k: k.replace(path, "").split("/", 2)[0],
            db.get(path, keys=True)[1] or []
        )))

        return {k: obj(self, k) for k in keys}

    def path(self):
        return self.path_str.format(self.parent, self.name)


class Cluster(Model):
    path_str = "{}/clusters/{}"
    parent   = "service/znappy"

    def __init__(self, name):
        self.name   = name
        self.config = self._load_config()
        self.hosts = self._load_children("{}/hosts/", Host)

    def _load_config(self):
        index, data = db.get("{}/config".format(self.path()))

        return json.loads(data['Value']) if data else {}

    def save(self):
        db.put(self.path(), None)

        for host in self.hosts:
            host.save()

    def lock(self):
        # TODO Test if we can implement a wait_for_lock
        return db.acquire("{}/.lock".format(self.path()))

    def release(self):
        return db.release("{}/.lock".format(self.path()))


class Host(Model):
    path_str = "{}/hosts/{}"

    def __init__(self, cluster, name = None):
        self.name      = name or db.node
        self.cluster   = cluster
        self.parent    = cluster.path()
        self.snapshots = self._load_children("{}/snapshots/", Snapshot)

    def save(self):
        db.put('{}/snapshots/'.format(self.path()), None)

        # add it to the cluster, to prevent reloading
        self.cluster.hosts[self.name] = self

        for snapshot in self.snapshots:
            snapshot.save()


class Snapshot(Model):
    path_str = "{}/snapshots/{}"

    def __init__(self, host, name = None):
        self.name    = name
        self.host    = host
        self.parent  = host.path()

        if self.name is not None:
            self.driver, self.target, self.time = self._load()

    def _load(self):
        index, data = db.get(self.path())

        if data:
            snapshot = json.loads(data['Value'])
            return map(snapshot.get, ['driver', 'target', 'time'])
        else:
            return {}

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

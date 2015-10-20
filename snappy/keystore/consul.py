from __future__ import absolute_import
import json
import time
from StringIO import StringIO
from snappy.keystore.base import BaseKeystore
import consul


class ConsulKeystore(BaseKeystore):
    def __init__(self, config):
        self.consul   = consul.Consul(**config)


    def _to_json(self, s):
        return json.load(StringIO(s['Value']))


    def connect(self):
        self.session_id = self.consul.session.create(name="snappy-agent-keystore")
        self.node = self.consul.session.info(self.session_id)[1]['Node']


    def close(self):
        self.consul.session.destroy(self.session_id)


    def list_snapshots(self, **kwargs):
        index, snapshots = self.consul.kv.get(
            "service/snappy/snapshots/{0}".format(self.node),
            recurse=True
        )

        if snapshots is None:
            return None

        candidates = map(self._to_json, snapshots)

        for k, v in kwargs.iteritems():
            candidates = filter(lambda x: x[k] == v, candidates)

        return candidates


    def add_snapshot(self, **kwargs):
        snapshot = {
            "filesystem": None,
            "name": "default-snapshot-name",
            "time": int(time.time()),
        }
        
        snapshot.update(kwargs)

        return self.consul.kv.put(
            "service/snappy/snapshots/{0}/{1}".format(self.node, snapshot['name']),
            json.dumps(snapshot)
        )


    def get_config(self):
        return {}

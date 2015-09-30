from __future__ import absolute_import
import json
import time
from StringIO import StringIO
from snappy.keystore.base import BaseKeystore
import consul


class ConsulKeystore(BaseKeystore):
    def __init__(self, config):
        self.consul   = consul.Consul(**config)


    def __enter__(self):
        self.session_id = self.consul.session.create(name="snappy-agent-keystore")
        self.node = self.consul.session.info(self.session_id)[1]['Node']

        return self

    def __exit__(self, type, value, tb):
        self.consul.session.destroy(self.session_id)


    def list_snapshots(self):
        index, snapshots = self.consul.kv.get(
            "service/snappy/snapshots/{0}".format(self.node),
            recurse=True
        )

        if snapshots is None:
            return [{'time':0}]
        else:
            return map(lambda x: json.load(StringIO(x['Value'])), snapshots)


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

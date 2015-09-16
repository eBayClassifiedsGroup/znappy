from __future__ import absolute_import
from snappy.lockagent.base import BaseLockAgent
import consul

class ConsulLockAgent(BaseLockAgent):
    def __init__(self, config):
        self.consul = consul.Consul(**config)


    def __enter__(self):
        self.session_id = self.consul.session.create(name="snappy-agent-lockagent")

        return self


    def __exit__(self, type, value, tb):
        self.release()
        self.consul.session.destroy(self.session_id)

    def acquire(self):

        return self.consul.kv.put(
            "service/snappy/.lock",
            "",
            acquire=self.session_id
        )


    def release(self):
        return True

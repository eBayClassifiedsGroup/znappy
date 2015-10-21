from __future__ import absolute_import
from snappy.lockagent.base import BaseLockAgent
import consul
import logging

logger = logging.getLogger(__name__)

class ConsulLockAgent(BaseLockAgent):
    def __init__(self, config):
        logger.debug("init lockagent")
        self.consul = consul.Consul(**config)


    def connect(self):
        logger.debug('connect lockagent')
        self.session_id = self.consul.session.create(name="snappy-agent-lockagent")


    def close(self):
        self.release()
        self.consul.session.destroy(self.session_id)


    def acquire(self):
        return self.consul.kv.put(
            "service/snappy/.lock",
            "",
            acquire=self.session_id
        )


    def release(self):
        return self.consul.kv.put(
            "service/snappy/.lock",
            "",
            release=self.session_id
        )

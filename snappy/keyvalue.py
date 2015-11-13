import consul
import logging

logger = logging.getLogger(__name__)


class KeyValue(object):
    def __init__(self, **kwargs):
        self._consul = consul.Consul(**kwargs)
        self.node    = None


    def __enter__(self):
        self.connect()


    def __exit__(self, type, value, tb):
        self.close()


    def connect(self):
        self._session_id = self._consul.session.create(name="snappy-agent")

        logger.debug("Connected to Consul: {}".format(self._session_id))

        # not atomic, could in theory fail
        try:
            self.node = self._consul.session.info(self._session_id)[1]['Node']
        except Exception, e:
            return False

        return True


    def close(self):
        self._consul.session.destroy(self._session_id)


    def get(self, *args, **kwargs):
        return self._consul.kv.get(*args, **kwargs)


    def put(self, *args, **kwargs):
        return self._consul.kv.put(*args, **kwargs)


    def delete(self, *args, **kwargs):
        return self._consul.kv.delete(*args, **kwargs)


    def acquire(self, lock, *args, **kwargs):
        logger.debug("trying to acquire lock {0} for {1}".format(lock, self._session_id))
        # TODO implement semaphore
        return self._consul.kv.put(lock, "", acquire=self._session_id)


    def release(self, lock, *args, **kwargs):
        return self._consul.kv.put(lock, "", release=self._session_id)

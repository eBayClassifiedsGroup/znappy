import consul
import logging
import time
import sys

logger = logging.getLogger(__name__)


class KeyValue(object):
    def __init__(self, dc=None, **kwargs):
        self._consul = consul.Consul(**kwargs)
        self.dc      = None
        self.node    = None

    def __enter__(self):
        self.connect()

    def __exit__(self, type, value, tb):
        self.close()

    def connect(self):
        logger.info('Connecting to consul and starting new session')
        self._session_id = self._consul.session.create(name="znappy-agent", ttl=30, lock_delay=0)

        logger.debug("Connected to Consul: {}".format(self._session_id))

        # not atomic, could in theory fail
        try:
            info = self._consul.session.info(self._session_id)
            logger.debug(info)
            self.node = info[1]['Node']
        except Exception:
            return False

        if not self.dc:
            try:
                self.dc = self._consul.agent.self()['Config']['Datacenter']
            except Exception:
                return False

        return True

    def close(self):
        try:
            self._consul.session.destroy(self._session_id)
        except:
            pass

    def ping(self):
        while True:
            try:
                return self._consul.session.renew(self._session_id)
            except consul.base.NotFound:
                # session expired/not found
                logger.warn('Failed to renew session {}, the session is probably expired'.format(self._session_id))

                # try to start a new session, otherwise give up
                if not self.connect():
                    # exit with exit code 1, so that upstart will try to restart the service with a clean start
                    logger.fatal('Failed to start new session :( giving up')
                    sys.exit(1)
            except Exception, e:
                # this error may be recoverable, will try a couple of times
                # this loop is not really infite, it will, at some point, get a session not found exception
                # and will hit the except above
                logger.warn('Failed to renew session {} with error {}, will keep trying'.format(self._session_id, e.message))
                time.sleep(1)

    def get(self, *args, **kwargs):
        return self._consul.kv.get(*args, dc=self.dc, **kwargs)

    def put(self, *args, **kwargs):
        while True:
            try:
                return self._consul.kv.put(*args, dc=self.dc, **kwargs)
            except Exception:
                logger.warn('Failed to update key {}'.format(args[0]))
                time.sleep(1)

    def delete(self, *args, **kwargs):
        return self._consul.kv.delete(*args, dc=self.dc, **kwargs)

    def acquire(self, lock, *args, **kwargs):
        logger.debug("trying to acquire lock {0} for {1}".format(lock, self._session_id))
        # TODO implement semaphore
        try:
            result = self._consul.kv.put(lock, "", acquire=self._session_id, dc=self.dc)
            logger.debug(result)
        except Exception:
            # if the put fails for whatever reason, we didn't get it
            return False
        return result

    def release(self, lock, *args, **kwargs):
        return self._consul.kv.put(lock, "", release=self._session_id, dc=self.dc)

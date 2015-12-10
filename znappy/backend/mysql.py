""" Package for managing MySQL Backend
"""

from fabric.api import task, local
import logging
import MySQLdb
import MySQLdb.cursors

__all__ = ['load_handlers']

logger = logging.getLogger(__name__)


class MySQL(object):
    def __init__(self, config):
        self.config = config
        self.conn = MySQLdb.connect(
            host='localhost',
            user=config.get('user', 'root'),
            passwd=config.get('password', None),
            cursorclass=MySQLdb.cursors.DictCursor
        )

    def __del__(self):
        """On dereference try to destroy connection"""
        try:
            self.close()
        except:
            pass

    def close(self):
        self.conn.close()

        return True, ""

    @task
    def stop_mysql(self):
        return local('service mysql stop').return_code == 0, ""

    @task
    def start_mysql(self):
        return local('service mysql start').return_code == 0, ""

    def i_am_master(self):
        read_only = self.query('SHOW GLOBAL VARIABLES LIKE "read_only"')[0]['Value']
        if read_only != 'OFF':
            logger.debug("Host is read-only, so it isn't the master")
            return False
        slave_status = self.query('SHOW SLAVE STATUS')
        if slave_status:
            logger.debug("Host is a slave to {}, so it isn't the master".format(slave_status[0]['Master_Host']))
            return False
        slaves = self.query('SHOW SLAVE HOSTS')
        if not slaves:
            logger.debug("Host doesn't have any slaves, so it probably isn't the master")
            return False
        return True

    def lock_mysql(self):
        logger.debug('Locking tables')
        self.query('FLUSH LOCAL TABLES WITH READ LOCK')
        return True, ""

    def unlock_mysql(self):
        logger.debug('Unlocking tables')
        self.query('UNLOCK TABLES')
        return True, ""

    def stop_replication(self):
        logger.debug('Stopping replication')
        self.query('STOP SLAVE SQL_THREAD')
        return True, ""

    def start_replication(self):
        logger.debug('Starting replication')
        self.query('START SLAVE SQL_THREAD')
        return True, ""

    def flush_tables(self):
        logger.debug('Flushing tables')
        self.query('FLUSH TABLES')
        return True, ""

    def query(self, query):
        cursor = self.conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()

    def stop(self):
        self.stop_mysql(self)

    def start(self):
        self.start_mysql(self)

    def check_master(self):
        if self.i_am_master():
            return False, "Host is a master, no snapshots will be created"
        return True, ""

    def start_snapshot(self):
        return self.lock_mysql()

    def monitor(self):
        if self.i_am_master():
            return False, "OK: Host is a MySQL master"
        else:
            return True, "OK: Host is a MySQL slave"

    def end_snapshot(self):
        return self.unlock_mysql()


def load_handlers(config, keystore, register):
    logger.debug('called with config: {}'.format(config))

    mysql = MySQL(config)
    # agent
    register("pre_snapshot", mysql.check_master)
    register("start_snapshot", mysql.start_snapshot)
    register("end_snapshot", mysql.end_snapshot)
    register("post_snapshot", mysql.close)

    # restore
    register("start_restore", mysql.stop)
    register("end_restore", mysql.start)

    # Monitor
    register("monitor", mysql.monitor)

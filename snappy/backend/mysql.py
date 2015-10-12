from snappy.backend.base import BaseBackend
from snappy.utils import logger
from fabric.api import task, local, env

import MySQLdb

class MySQLBackend(BaseBackend):
    @task
    def stop_mysql(self):
        return local('service mysql stop')


    @task
    def start_mysql(self):
        return local('service mysql start')


    def lock_mysql(self):
        logger.debug('Locking tables')
        return self.query('FLUSH LOCAL TABLES WITH READ LOCK;')


    def unlock_mysql(self):
        logger.debug('Unlocking tables')
        return self.query('UNLOCK TABLES')


    def _connect(self):
        return MySQLdb.connect(
                        host=self.config['host'],
                        user=self.config['user'],
                        passwd=self.config['password']
        )

    def __init__(self, config):
        env.host_string = 'localhost'

        self.config = config


    def start_snapshot(self):
        # should also remove from loadbalancer
        return self.lock_mysql()


    def end_snapshot(self):
        # should also add back to the loadbalancer
        return self.unlock_mysql()


    def start_restore(self):
        self.stop_mysql(self)


    def end_restore(self):
        self.start_mysql(self)
        #self.start_replication()
    

    def query(self, query):
        mysql = self._connect()

        cursor = mysql.cursor()
        cursor.execute(query)

        mysql.close()

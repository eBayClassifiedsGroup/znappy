import time
from snappy.utils import logger
from fabric.api import env, local, output, taks, settings

class MySQLSnapshot:
    def __init__(self, keystore, config = None, name = None):
        self.keystore   = keystore



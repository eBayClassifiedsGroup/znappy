from snappy.backend.base import BaseBackend
from snappy.utils import logger

class MockBackend(BaseBackend):
    def __init__(self, config = None):
        pass


    def start_snapshot(self):
        logger.debug('Start mockkkkking')
        return True


    def end_snapshot(self):
        logger.debug('End mockkkkking')
        return True


    def start_restore(self):
        logger.debug('Start mocccckkkking')
        return True


    def end_restore(self):
        logger.debug('Stop moccccckkkkkkiinnng')
        return True 

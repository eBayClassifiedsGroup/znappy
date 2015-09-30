from . import utils


class Snapshot:
    def __init__(self, keystore, config):
        self.keystore = keystore
        self.config   = config


    def create(self, filesystem):
        utils.logger.debug('creating snapshot')


    def save(self):
        utils.logger.debug('saving snapshot')


    def restore(self, name):
        utils.logger.debug('restoring snapshot')

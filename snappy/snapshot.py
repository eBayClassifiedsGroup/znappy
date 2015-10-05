import time
from . import utils
from fabric.api import env, local, output, task, settings


class Snapshot:
    @task
    def zfs_snapshot_create(self):
        self.time = int(time.time())
        self.name = "{0}@{1}".format(self.filesystem, self.time)

        return local('zfs snap {}'.format(self.name))


    def __init__(self, keystore, config = None, name = None):
        self.keystore   = keystore

        #configure fabric
        env.host_string   = 'localhost'
        env.warn_only     = True

        for c in ['running', 'stderr', 'status', 'warning']:
            output[c] = False

        # check if we have all the required params
        if not (config or name):
            utils.logger.critical('Snapshot required either config or name to load')
            exit(1)

        if config:
            self._loaded    = False
            self.filesystem = config['filesystem']
            self.config     = config
            self.name       = None
        else:
            # do something to load the snapshot from the keystore
            pass


    def __repr__(self):
        return str({
                'time': self.time,
                'filesystem': self.filesystem,
                'name': self.name
        })

    def create(self):
        utils.logger.debug('creating snapshot')
        result = self.zfs_snapshot_create(self)

        if not result.return_code == 0:
            utils.logger.fatal('Snapshot failed :(')
            # raise Exception? we don't have any :(
            exit(0)

        utils.logger.info('Snapshot created succesfully')
        self._loaded = True

        return result

    def list(self):
        utils.logger.debug('listing snapshot for filesystem {}'.format(self.filesystem))


    def save(self):
        if not self._loaded:
            utils.logger.error('Cannot save snapshot, no snapshot loaded!')
            return False

        utils.logger.debug('saving snapshot')

        return self.keystore.add_snapshot(
            filesystem=self.filesystem,
            name=self.name,
            time=self.time
        )


    def restore(self, name):
        utils.logger.debug('restoring snapshot')

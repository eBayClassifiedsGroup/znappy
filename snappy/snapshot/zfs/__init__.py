import time
from snappy.utils import register_handler
from snappy.snapshot import Snapshot
from fabric.api import env, local, output, task

import logging

logger = logging.getLogger(__name__)


class ZFSSnapshot(object):
    @task
    def zfs_snapshot_clone(self, clone_name):
        logger.debug('Cloning {0} into {1}'.format(self.name, clone_name))

        if local('zfs clone {0} {1}'.format(self.name, clone_name)).return_code != 0:
            raise SnapshotException('Failed to clone snapshot')


    @task
    def zfs_snapshot_promote(self, clone_name):
        logger.debug('Promoting {}'.format(clone_name))

        if local('zfs promote {}'.format(clone_name)).return_code != 0:
            raise SnapshotException('Failed to promote snapshot')


    @task
    def zfs_snapshot_create(self):
        self.time = int(time.time())
        self.name = "{0}@{1}".format(self.filesystem, self.time)

        return local('zfs snap {}'.format(self.name))


    def _config_fabric(self):
        env.host_string = 'localhost'
        env.warn_only   = True

        for c in ['running', 'stderr', 'status', 'warning']:
            output[c] = False


    def __init__(self, snapshot, config):
        logger.debug("init zfssnapshot with config: {}".format(config))

        self.snapshot   = snapshot
        self.filesystem = config['filesystem']

        self._config_fabric()


    def create(self):
        logger.debug('creating snapshot')
        result = self.zfs_snapshot_create(self)

        if result.return_code == 0:
            logger.info('Snapshot created succesfully')
        else:
            logger.fatal('Snapshot failed :(')

        return result.return_code == 0, ''


    def list(self):
        logger.debug('listing snapshot for filesystem {}'.format(self.filesystem))


    def save(self):
        return self.snapshot.save(**{
            'name':   self.name,
            'time':   self.time,
            'target': self.filesystem
        }), 'zfssnapshot_save'

    def restore(self):
        logger.debug('restoring snapshot')

        clone_name = "{0}_clone-{1}".format(self.filesystem, int(time.time()))

        # TODO check if there are any other open filehandles to the mountpoint, otherwise this sequence will FAIL
        try:
            self.zfs_snapshot_clone(self, clone_name)

            self.zfs_snapshot_promote(self, clone_name)

            self.zfs_snapshot_destroy(self)

            self.zfs_snapshot_rename(self, clone_name)
        except SnapshotException, e:
            # TODO fix this with rollback and checks
            logger.fatal(e)


class SnapshotException(Exception):
    pass


def load_handlers(config, keystore):
    logger.debug('called with config: {}'.format(config))

    # TODO do not pass the keystore to the ZFS module, rather
    #      create a snapshot instance and pass that, making the module
    #      transparent to changes in the snapshot details
    instance = ZFSSnapshot(Snapshot(keystore), config=config)
    
    register_handler("create_snapshot", instance.create)
    register_handler("save_snapshot", instance.save)

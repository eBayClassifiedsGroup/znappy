import time
from snappy.utils import keystore, register_handler
from snappy.snapshot.base import BaseSnapshot
from fabric.api import env, local, output, task, settings

import logging

logger = logging.getLogger(__name__)


class ZFSSnapshot(BaseSnapshot):
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


    def create(self):
        logger.debug('creating snapshot')
        result = self.zfs_snapshot_create(self)

        if not result.return_code == 0:
            logger.fatal('Snapshot failed :(')
            # raise Exception? we don't have any :(
            exit(0)

        logger.info('Snapshot created succesfully')
        self._loaded = True

        return result

    def list(self):
        logger.debug('listing snapshot for filesystem {}'.format(self.filesystem))


    def save(self):
        if not self._loaded:
            logger.error('Cannot save snapshot, no snapshot loaded!')
            return False

        logger.debug('saving snapshot')

        return self.keystore.add_snapshot(
            filesystem=self.filesystem,
            name=self.name,
            time=self.time
        )


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
            # fix this with rollback and checks
            logger.fatal(e)


def load_handlers(config):
    logger.debug('called with config: {}'.format(config))
    instance = ZFSSnapshot(keystore, config=config)
    
    register_handler("create_snapshot", instance.create)
    register_handler("save_snapshot", instance.save)

class SnapshotException(Exception):
    pass

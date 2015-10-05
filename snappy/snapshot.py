import time
from snappy.utils import logger
from fabric.api import env, local, output, task, settings


class Snapshot:
    @task
    def zfs_snapshot_clone(self, clone_name):
        logger.debug('Cloning {0} into {1}'.format(self.name, clone-name))

        if local('zfs clone {0} {1}'.format(self.name, clone_name)).return_code != 0:
            raise SnapshotException('Failed to clone snapshot')


    @task
    def zfs_snapshot_promote(self, clone_name):
        logger.debug('Promoting {}'.format(clone_name)

        if local('zfs promote {}'.format(clone_name)).return_code != 0:
            raise SnapshotException('Failed to promote snapshot')


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
            logger.critical('Snapshot required either config or name to load')
            exit(1)

        if config:
            self._loaded    = False
            self.filesystem = config['filesystem']
            self.config     = config
            self.name       = None
        else:
            data = keystore.list_snapshots(name=name)

            if len(data) == 0:
                raise KeyError('Snapshot with name {} not found'.format(name))

            self._loaded    = True
            self.time       = data[0]['time']
            self.filesystem = data[0]['filesystem']
            self.config     = data[0]
            self.name       = name

    def __repr__(self):
        return str({
                'time': self.time,
                'filesystem': self.filesystem,
                'name': self.name
        })

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


    def restore(self, name):
        logger.debug('restoring snapshot')

        clone_name = "{0}_clone-{1}".format(self.name, int(time.time()))

        # TODO check if there are any other open filehandles to the mountpoint, otherwise this sequence will FAIL

        try:
            self.zfs_snapshot_clone(self, clone_name)

            self.zfs_snapshot_promote(self, clone_name)

            self.zfs_snapshot_destroy(self)

            self.zfs_snapshot_rename(self, clone_name)
        except SnapshotError, e:
            # fix this with rollback and checks
            logger.fatal('Oops.. you probably still have some filehandles open to {}'.format(self.filesystem))
            exit(1)


class SnapshotError(Exception):
    pass

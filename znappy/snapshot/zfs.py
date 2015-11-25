import time
from znappy.utils import register_handler
from fabric.api import env, local, output, task

import logging
import sys

logger = logging.getLogger(__name__)


class ZFSSnapshot(object):
    @task
    def zfs_snapshot_clone(self, clone_name):
        logger.debug('Cloning {filesystem}@{time} into {}'.format(clone_name, **dict(self)))

        if local('/sbin/zfs clone {filesystem}@{time} {}'.format(clone_name, **dict(self))).return_code != 0:
            raise SnapshotException('Failed to clone snapshot')


    @task
    def zfs_snapshot_promote(self, clone_name):
        logger.debug('Promoting {}'.format(clone_name))

        if local('/sbin/zfs promote {}'.format(clone_name)).return_code != 0:
            raise SnapshotException('Failed to promote snapshot')


    @task
    def zfs_snapshot_create(self):
        self.time = int(time.time())
        self.name = "{driver}-{filesystem}-{time}".format(**self.__dict__).replace('/','.')

        return local('/sbin/zfs snap {filesystem}@{name}'.format(**self.__dict__))


    @task
    def zfs_snapshot_destroy(self, target):
        if local('/sbin/zfs destroy {}'.format(target)).return_code != 0:
            raise SnapshotException('Failed to destroy snapshot')


    def _config_fabric(self):
        env.host_string = 'localhost'
        env.warn_only   = True

        for c in ['running', 'stderr', 'status', 'warning']:
            output[c] = False


    def __init__(self, snapshot, config):
        logger.debug("init zfssnapshot with config: {}".format(config))

        self.driver     = __name__
        self.snapshot   = snapshot
        self.filesystem = config['filesystem']
        self.rotate     = config.get('rotate', 12)

        self._config_fabric()


    def create(self, *args, **kwargs):
        logger.debug('creating snapshot')
        result = self.zfs_snapshot_create(self)

        if result.return_code == 0:
            logger.info('Snapshot created succesfully')
            self.snapshot.name   = self.name
            self.snapshot.driver = self.driver
            self.snapshot.target = self.filesystem
            self.snapshot.time   = self.time
        else:
            logger.fatal('Snapshot failed :(')

        return result.return_code == 0, ''


    def cleanup(self, snapshots, *args, **kwargs):
        """Snapshots are sorted new -> old by default
        """

        if len(snapshots) <= self.rotate:
            return True, 'No snapshots need to be deleted'

        snapshots = snapshots[self.rotate:]

        for s in snapshots:
            try:
                logger.debug("Deleting snapshot: {}".format(s.name))
                self.zfs_snapshot_destroy(self, "{0}@{1}".format(s.target, s.name))
            except SnapshotException, e:
                # It may be that the snapshot is still in the keystore, but not
                # on the filesystem/zfs, ignore these errors for now
                pass

            s.delete()

        return True, 'cleanup complete'


    def save(self, *args, **kwargs):
        return self.snapshot.save(), 'zfssnapshot_save'


    def start_restore(self, snapshot, *args, **kwargs):
        cmd = local("fuser -m /$(zfs get -H -o value mountpoint {})".format(snapshot.target))

        logger.debug(cmd)
        
        return True, ''

    def restore(self, *args, **kwargs):
        logger.debug('restoring snapshot')

        clone_name = "{0}_clone-{1}".format(self.filesystem, int(time.time()))

        logger.debug(clone_name)

        # TODO check if there are any other open filehandles to the mountpoint, otherwise this sequence will FAIL
        try:
            #self.zfs_snapshot_clone(self, clone_name)

            #self.zfs_snapshot_promote(self, clone_name)

            #self.zfs_snapshot_destroy(self, self.name)

            #self.zfs_snapshot_rename(self, clone_name)
            pass
        except SnapshotException, e:
            # TODO fix this with rollback and checks
            logger.fatal(e)

        return True, ''


class SnapshotException(Exception):
    pass


def load_handlers(config, snapshot, register=register_handler):
    logger.debug('called with config: {}'.format(config))

    instance = ZFSSnapshot(snapshot, config=config)
    
    # handlers for creating a snapshot
    register("create_snapshot", instance.create)
    register("save_snapshot", instance.save)
    register("delete_snapshot", instance.cleanup)

    # handlers for restoring a snapshot
    register("start_restore", instance.start_restore, priority=sys.maxint)   # some high number, want to do this as last
    register("do_restore", instance.restore)

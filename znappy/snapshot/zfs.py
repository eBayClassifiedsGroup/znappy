import time
from znappy.utils import register_handler
from fabric.api import env, local, output, task

import logging
import sys

logger = logging.getLogger(__name__)


class ZFSSnapshot(object):
    @task
    def zfs_get_properties(self, target):
        if self.properties:
            properties = ','.join(self.properties)
            cmd = '/sbin/zfs get -H -o property,value {properties} {target}'.format(
                properties=properties,
                target=target
            )

            logger.debug('Getting properties from {}'.format(target))
            logger.debug('Executing {}'.format(cmd))

            result = local(cmd, capture=True)

            if result.return_code != 0:
                raise SnapshotException('Failed to get properties of {}'.format(target))

            props = {k.split()[0]: k.split()[1] for k in result.splitlines()}
            return props

        return {}

    @task
    def zfs_unmount(self, target):
        cmd = '/sbin/zfs unmount {}'.format(target)

        logger.debug('Unmounting {}'.format(target))
        logger.debug('Executing: {}'.format(cmd))

        if local(cmd).return_code != 0:
            raise SnapshotException('Failed to unmount filesystem')

    @task
    def zfs_clone(self, source, target, properties):
        prop_list = ' '.join(map(lambda p: "-o {}={}".format(p, properties[p]), properties))

        cmd = '/sbin/zfs clone {prop_list} {source} {target}'.format(
            prop_list=prop_list,
            source=source,
            target=target
        )

        logger.debug('Cloning {source} into {target}'.format(
            source=source,
            target=target
        ))

        logger.debug('Executing: {}'.format(cmd))

        if local(cmd).return_code != 0:
            raise SnapshotException('Failed to clone {}'.format(source))

    @task
    def zfs_promote(self, target):
        cmd = '/sbin/zfs promote {target}'.format(target=target)

        logger.debug('Promoting {}'.format(target))
        logger.debug('Executing: {}'.format(cmd))

        if local(cmd).return_code != 0:
            raise SnapshotException('Failed to promote {}'.format(target))

    @task
    def zfs_rename(self, source, target):
        cmd = '/sbin/zfs rename {source} {target}'.format(
            source=source,
            target=target
        )

        logger.debug('Renaming {source} to {target}'.format(source=source, target=target))
        logger.debug('Executing: {}'.format(cmd))

        if local(cmd).return_code != 0:
            raise SnapshotException('Failed to clone snapshot')

    @task
    def zfs_snapshot_create(self, target):
        create_time = int(time.time())
        name = "{driver}-{target}-{time}".format(
            driver=self.driver,
            filesystem=target.replace('/', '.'),
            time=create_time
        )

        cmd = '/sbin/zfs snap {target}@{name}'.format(
            target=target,
            name=name
        )

        if local(cmd).return_code != 0:
            raise SnapshotException('Failed to create snapshot of {}'.format(target))

        return (name, create_time)

    @task
    def zfs_destroy(self, target):
        cmd = '/sbin/zfs destroy -r {target}'.format(target=target)

        logger.debug('Destroying {}'.format(target))
        logger.debug('Executing: {}'.format(cmd))

        if local(cmd).return_code != 0:
            raise SnapshotException('Failed to destroy {}'.format(target))

    @task
    def zfs_snapshot_list(self, target):
        result = local('/sbin/zfs get -H -o value name -t snapshot {target}'.format(target=target), capture=True)

        if result.return_code != 0:
            raise SnapshotException('Failed to list snapshots')

        return result.split('\n')

    def _config_fabric(self):
        env.host_string = 'localhost'
        env.warn_only   = True

        for c in output.keys():
            output[c] = False

    def __init__(self, config):
        logger.debug("init zfssnapshot with config: {}".format(config))

        self.driver     = __name__
        self.filesystem = config['filesystem']
        self.properties = config.get('properties', [])

        self._config_fabric()

    def create(self, snapshot=None, *args, **kwargs):
        if not snapshot:
            return False, 'No snapshot given!'

        logger.debug('creating snapshot')
        result = self.zfs_snapshot_create(self, self.filesystem)

        try:
            name, create_time = self.zfs_snapshot(self)

            snapshot.driver = self.driver
            snapshot.name = name
            snapshot.target = self.filesystem
            snapshot.time = create_time
        except:
            logger.fatal('Snapshot failed :(')
            return False, 'Failed to create snapshot'
            
        return True, 'snapshot created'

    def delete_snapshot(self, snapshot=None, *args, **kwargs):
        try:
            snapshot_name = "{}@{}".format(snapshot.target, snapshot.name)
            logger.debug("Deleting snapshot: {}".format(s.name))
            self.zfs_destroy(self, snapshot_name)
        except SnapshotException:
            # It may be that the snapshot is still in the keystore, but not
            # on the filesystem/zfs, ignore these errors for now
            pass

        return True, 'zfs_snapshot deleted'

    def start_restore(self, snapshot=None, *args, **kwargs):
        if not snapshot:
            return False, 'No snapshot to restore'

        cmd = local("fuser -k -9 -m /$(zfs get -H -o value mountpoint {})".format(snapshot.target))

        logger.debug(cmd)

        return True, ''

    def restore(self, snapshot=None, *args, **kwargs):
        if not snapshot:
            return False, 'No snapshot to restore'

        logger.debug('restoring snapshot')

        # for example data/mysql_clone-12345678
        clone_name = "{0}_clone-{1}".format(self.filesystem, int(time.time()))

        snapshot_name = "{}@{}".format(snapshot.target, snapshot.name)

        properties = self.zfs_get_properties(snapshot.target)

        logger.debug(clone_name)

        # TODO check if there are any other open filehandles to the mountpoint, otherwise this sequence will FAIL
        try:
            self.zfs_unmount(self, self.filesystem)
            self.zfs_clone(self, snapshot_name, clone_name, properties)
            self.zfs_snapshot_promote(self, clone_name)
            self.zfs_destroy(self, self.filesystem)
            # set mountpoint
            self.zfs_rename(self, clone_name, self.filesystem)
        except SnapshotException, e:
            # TODO fix this with rollback and checks
            logger.fatal(e)
            return False, 'Failed to execute zfs::restore'

        return True, 'restore complete'

    def check_snapshot_sync(self, znappy, *args, **kwargs):
        local_snapshots = self.zfs_list(self, self.filesystem)

        local_snapshots = set(x.split('@', 2)[1] for x in local_snapshots)
        consul_snapshots = set(znappy.host.snapshots.keys())

        logger.debug(local_snapshots)
        logger.debug(consul_snapshots)

        diff = local_snapshots ^ consul_snapshots

        if len(diff) == 0:
            return True, (0, "OK: No differences in consul and local system")
        elif len(diff) == 1:
            return False, (1, "WARN: {} difference between consul and local system: {}".format(len(diff), ', '.join(diff)))
        else:
            return False, (2, "CRITICAL: {} differences between consul and local system: {}".format(len(diff), ', '.join(diff)))


class SnapshotException(Exception):
    pass


def load_handlers(config, register):
    logger.debug('called with config: {}'.format(config))

    instance = ZFSSnapshot(config)

    # handlers for creating a snapshot
    register("create_snapshot", instance.create)
    register("delete_snapshot", instance.delete_snapshot)

    # handlers for monitoring
    register("monitor", instance.check_snapshot_sync)

    # handlers for restoring a snapshot
    register("start_restore", instance.start_restore, priority=sys.maxint)   # some high number, want to do this as last
    register("do_restore", instance.restore)

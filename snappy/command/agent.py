"""
Usage:
    snappy agent [options]

Options:
    -f, --force  Force create a snapshot, this bypasses the pre_/post_ phase and the locking agent
"""

from snappy import backend, keystore, lockagent, snapshot
from snappy.utils import config, logger

import time


def check_update(ks, config):
    snapshots = ks.list_snapshots()

    logger.debug(snapshots)

    last_snapshot = max(snapshots, key=lambda v:v['time'])

    if last_snapshot['time'] < int(time.time() - config['max_age']):
        logger.debug('last snaphot ({0}) is older then {1} seconds'.format(last_snapshot['time'], config['max_age']))
        return True
    else:
        logger.debug('last snapshot ({0}) is ok'.format(last_snapshot['time']))
        return False


def main(args):
    logger.debug("Using arguments: {0}".format(args))
    logger.debug("Using configuration: {0}".format(config))

    # get keystore and lockagent
    with keystore.get(*config['keystore']) as ks, lockagent.get(*config['lockagent']) as la:
        # preflight check
        if check_update(ks, config['snapshot']) and la.acquire():
            try:
                # notify the backend that we are about to start a snapshot
                be = backend.get(*config['backend'])
                be.start_snapshot()

                snap = snapshot.get('ZFSSnapshot', ks, config['snapshot'])
                snap.create()

                logger.debug(snap)

                snap.save()
            except Exception as e:
                logger.debug(e)

                # roll back stuff
            finally:
                # notify the backend we are done making the snapshot
                if be:
                    be.end_snapshot()
        else:
            logger.debug('Preflight failure.. skipping run')

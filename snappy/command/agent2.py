"""
Usage:
    snappy agent2 [options]

Options:
    -f, --force  Force create a snapshot, this bypasses the pre_/post_ phase and the locking agent
"""

from snappy import keystore, lockagent, snapshot
from snappy.utils import config, execute_event
from snappy.utils import keystore as ks
from snappy.utils import lockagent as la

import importlib
import time
import logging

logger = logging.getLogger(__name__)

def check_update(ks, config):
    snapshots = ks.list_snapshots()

    logger.debug(snapshots)

    if snapshots is None:
        logger.debug('no snapshots in datastore')
        return True

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

    for pkgname in config['drivers'].keys():
        try:
            pkg = importlib.import_module(pkgname)
            pkg.load_handlers(config['drivers'].get(pkgname, {}))
        except ImportError, e:
            logger.info("Failed to load package: {}.. skipping".format(e.message))
        except Exception, e:
            logger.fatal("Failed to load package: {}.. exiting".format(e.message))
            raise e
            exit(1)

    # get keystore and lockagent
    with keystore.get(*config['keystore']) as ks, lockagent.get(*config['lockagent']) as la:
        # preflight check
        if check_update(ks, config['snapshot']) and la.acquire():
            try:
                # notify drivers that we are ready to start snapshotting
                execute_event('pre_snapshot', 'all_agent')
                execute_event('start_snapshot')

                # notify drivers that they can make the snapshot
                execute_event('create_snapshot')



#                snap = snapshot.get('ZFSSnapshot', ks, config['snapshot'])
#                snap = snapshot.get('MySQLSnapshot', ks, config['snapshot'])
#                snap.create()
#
#                logger.debug(snap)
#
#                snap.save()
            # just catch any exception, we want to make sure the finally does it's job
            except Exception as e:
                logger.debug(e)

            # roll back stuff
            finally:
                # we just made the snapshot
                # the drivers should take care that any of these steps may not have been executed
                execute_event('end_snapshot')
                execute_event('post_snapshot')
        else:
            logger.debug('Preflight failure.. skipping run')

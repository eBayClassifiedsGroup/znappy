"""
Usage:
    znappy agent [options]

Options:
    -f, --force    Force create a snapshot, this bypasses the pre_/post_ phase and the locking agent
"""

from znappy import models, utils
import time
import logging, logging.handlers


logger = logging.getLogger(__name__)
logger.addHandler(logging.handlers.SysLogHandler(address='/dev/log'))


def check_update(host, config):
    if not host.snapshots:
        logger.debug('no snapshots in datastore')
        return True

    last_snapshot = max(host.snapshots, key=lambda s: s.time)

    if last_snapshot.time < int(time.time() - config['min_age']):
        logger.debug('last snaphot ({0}) is older then {1} seconds'.format(last_snapshot.time, config['min_age']))
        return True
    else:
        logger.debug('last snapshot ({0}) is ok'.format(last_snapshot.time))
        return False


def clean_snapshots(host, config):
    """ Clean up all the snapshots that are outside the rotation
        This only needs to be done once per driver for all the 
        snapshots created with that driver.
    """
    for driver in config.get('drivers', []):
        logger.debug("running clean_snapshots for {}".format(driver))
        logger.debug(host.snapshots)

        # only get the snapshots for the current driver
        snapshots = filter(lambda s: s.driver == driver, host.snapshots)

        # sort snapshots by time DESC
        snapshots = sorted(snapshots, key=lambda s: s.time, reverse=True)

        # execute delete event for the current driver, with the snapshots created by this driver
        utils.execute_event(['delete_snapshot'], driver, snapshots)
    

def main(db, args):
    logger.debug("Using arguments: {0}".format(args))

    # create a cluster from the argument, use default is not specified
    cluster  = models.Cluster(args['--cluster'])
    host     = cluster.hosts[db.node]

    snapshot = models.Snapshot(host, None)

    # load drivers from 
    utils.load_drivers(cluster.config, snapshot)

    # preflight check
    # TODO the check_update should be per driver
    if check_update(host, cluster.config['snapshot']) and cluster.lock():
        try:
            # notify drivers that we are ready to start snapshotting
            utils.execute_event(['pre_snapshot', 'all_agent'])
            utils.execute_event(['start_snapshot'])

            # notify drivers that they can make the snapshot
            utils.execute_event(['create_snapshot'])
            # actually it might be better to not have this step and
            # have it in create_snapshot -> do_snapshot? to have atomicity
            utils.execute_event(['save_snapshot'])
        # just catch any exception, we want to make sure the finally does it's job
        except Exception as e:
            logger.debug(e)
            raise e

        # roll back stuff
        finally:
            # we just made the snapshot
            # the drivers should take care that any of these steps may not have been executed
            utils.execute_event(['end_snapshot'])
            utils.execute_event(['post_snapshot'])
    else:
        logger.debug('Preflight failure.. skipping run')

    # clean_snapshots(keystore, config)

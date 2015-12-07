"""
Restore a snapshot back to disk (mount)

Usage:
    znappy restore [options] <name>

Options:
    -f, --force                  Force restore of the snapshot, this will skip the lockagent
"""

from znappy import utils, models
import logging

logger = logging.getLogger(__name__)

def main(db, args):
    logger.debug("Using arguments: {}".format(args))

    cluster  = models.Cluster(args['--cluster'])
    host     = cluster.hosts[db.node]

    try:
        snapshot = host.snapshots[args['<name>']]
    except KeyError:
        logger.warn("Snapshot not found!")
        exit(1)

    logger.debug(snapshot)

    if args['--force'] or cluster.lock():
        try:
            logger.debug("Found snapshot: {}".format(snapshot))

            utils.load_drivers(cluster.config, snapshot)

            utils.execute_event(['pre_restore'], snapshot.driver)
            utils.execute_event(['start_restore'], snapshot.driver)

            utils.execute_event(['do_restore'], snapshot.driver)
        finally:
            utils.execute_event(['end_restore'])
            utils.execute_event(['post_restore'])

        # TODO add some cleanup function maybe
    else:
        logger.debug("Failed to start recovery!")

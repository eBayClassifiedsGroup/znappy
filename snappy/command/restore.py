"""
Restore a snapshot back to disk (mount)

Usage:
    snappy restore [options] <name>

Options:
    -c=<name>, --cluster=<name>  Name of the clusteri [default: default]
    -f, --force                  Force restore of the snapshot, this will skip the lockagent
"""

from snappy import utils, models
import logging

logger = logging.getLogger(__name__)

def main(db, args):
    config = utils.config

    logger.debug("Using arguments: {}".format(args))
    logger.debug("Using config: {}".format(config))

    cluster  = models.Cluster(args['--cluster'])
    host     = cluster.hosts[db.node]
    snapshot = filter(lambda s: s.name == args['<name>'], host.snapshots)

    logger.debug(snapshot)

    if len(snapshot) != 1:
        logger.info('Snapshot not found!')
    elif args['--force'] or cluster.lock():
        try:
            logger.debug("Found snapshot: {}".format(snapshot))

            utils.load_drivers(config, snapshot)

            utils.execute_event(['pre_restore'], snapshot.driver)
            utils.execute_event(['start_restore'], snapshot.driver)

            utils.execute_event(['do_restore'], snapshot.driver)
        finally:
            utils.execute_event(['end_restore'])
            utils.execute_event(['post_restore'])

        # TODO add some cleanup function maybe
    else:
        logger.debug("Failed to start recovery!")

"""
Usage:
    znappy snapshot
    znappy snapshot list [options]
    znappy snapshot restore <name>

Options:
    -h, --help                         Display this help
    -s=<column>, --sort=<column>       Output column to sort in [default: name]
    -r, --reverse                      Reverse the sorting of the output

Commands:
    list        List snapshots in the keystore
    restore     Restore a snapshot

"""

from znappy import utils, models
from prettytable import PrettyTable

import sys
import logging

logger = logging.getLogger(__name__)

def action_list(config, args, snapshots):
    if len(snapshots) == 0:
        print "No snapshots found on this machine"
        exit(0)

    logger.debug(snapshots)

    table = PrettyTable(fields=["name","driver","target","time"])

    for s in snapshots:
        table.add_row([s.name, s.driver, s.target, s.time])

    print table.get_string(sortby=args['--sort'], reversesort=args['--reverse'])


def action_restore(config, args, snapshots):
    logger.debug(snapshots)
    logger.debug(args)

    candidates = filter(lambda s: s.name == args['<name>'], snapshots)

    if len(candidates) != 1:
        logger.fatal('Snapshot name `{}` not found or ambiguous')
        exit(1)

    snapshot = candidates[0]

    utils.load_drivers(config, snapshot)

    try:
        utils.execute_event(['pre_restore'])
        utils.execute_event(['start_restore'])
        utils.execute_event(['do_restore'])
    except Exception, e:
        logger.fatal('Could not restore!')
    finally:
        utils.execute_event(['end_restore'])
        utils.execute_event(['post_restore'])


def main(db, args):
    logger.debug("Using arguments: {0}".format(args))

    module = sys.modules[__name__]

    cluster = models.Cluster(args['--cluster'])
    host    = models.Host(cluster)

    for c in ['list', 'restore']:
        if args[c] and hasattr(module, 'action_{}'.format(c)):
            command = getattr(module, 'action_{}'.format(c))
            command(cluster.config, args, host.snapshots)

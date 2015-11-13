"""
Usage:
    znappy snapshot
    znappy snapshot list [options]
    znappy snapshot restore <name>

Commands:
    list        List snapshots in the keystore
    restore     Restore a snapshot

Options:
    -h, --help                         Display this help
    -c=<cluster>, --cluster=<cluster>  Cluster name [default: default]
    -s=<column>, --sort=<column>       Output column to sort in [default: name]
    -r, --reverse                      Reverse the sorting of the output
"""

from znappy import utils, models
from prettytable import PrettyTable

import sys
import logging

logger = logging.getLogger(__name__)

def action_list(snapshots, args):
    if len(snapshots) == 0:
        print "No snapshots found on this machine"
        exit(0)

    logger.debug(snapshots)

    table = PrettyTable(field_names=["name","driver","target","time"])

    for s in snapshots:
        table.add_row([s.name, s.driver, s.target, s.time])

    print table.get_string(sortby=args['--sort'], reversesort=args['--reverse'])


def main(db, args):
    config = utils.config

    logger.debug("Using arguments: {0}".format(args))
    logger.debug("Using configuration: {0}".format(config))

    module = sys.modules[__name__]

    cluster = models.Cluster(args['--cluster'])
    host    = cluster.hosts[db.node]

    for c in ['list']:
        if args[c] and hasattr(module, 'action_{}'.format(c)):
            command = getattr(module, 'action_{}'.format(c))
            command(host.snapshots, args)

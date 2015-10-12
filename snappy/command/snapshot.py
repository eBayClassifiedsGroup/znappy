"""
Usage:
    snappy snapshot
    snappy snapshot list [options]
    snappy snapshot restore <name>

Commands:
    list        List snapshots in the keystore
    restore     Restore a snapshot

Options:
    -h, --help                    Display this help
    -s=<column>, --sort=<column>  Output column to sort in [default: name]
    -r, --reverse                 Reverse the sorting of the output
"""

from snappy import keystore, snapshot
from snappy.utils import config, logger
from prettytable import PrettyTable

import sys

def action_list(args, config):
    with keystore.get(*config['keystore']) as ks:
        snapshots = ks.list_snapshots()

        if len(snapshots) == 0:
            print "No snapshots found on this machine"
            exit(0)

        table = PrettyTable(field_names=["name","filesystem","time"])

        for s in snapshots:
            table.add_row([s['name'], s['filesystem'], s['time']])

        print table.get_string(sortby=args['--sort'], reversesort=args['--reverse'])


def action_restore(args, config):
    print 'restore!'
    pass

def main(args):
    logger.debug("Using arguments: {0}".format(args))
    logger.debug("Using configuration: {0}".format(config))

    module = sys.modules[__name__]

    for c in ['list', 'restore']:
        if args[c] and hasattr(module, 'action_{}'.format(c)):
            command = getattr(module, 'action_{}'.format(c))
            command(args,config)

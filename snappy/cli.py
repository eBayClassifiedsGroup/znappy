"""
Usage:
    snappy cli
    snappy cli snapshot list [options]

Options:
    -h, --help                    Display this help
    -s=<column>, --sort=<column>  Output column to sort in [default: name]
    -r, --reverse                 Reverse the sorting of the output
"""

from . import keystore, snapshot, utils
from prettytable import PrettyTable


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


def main(args):
    utils.get_logging('snappy-cli', args)

    utils.logger.debug("Using arguments: {0}".format(args))

    config = utils.get_config(args['--config'])

    utils.logger.debug("Using configuration: {0}".format(config))

    if args['list']:
        action_list(args, config)

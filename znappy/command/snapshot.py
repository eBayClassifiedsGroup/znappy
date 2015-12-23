"""
Usage:
    znappy snapshot create [--force]
    znappy snapshot list [--reverse] [--sort=<column>]
    znappy snapshot restore <name>
    znappy snapshot delete <name>
"""

from znappy import Znappy
from prettytable import PrettyTable

import sys
import logging

logger = logging.getLogger(__name__)


def action_create(znappy, args):
    znappy.run(force=args['--force'])


def action_list(znappy, args):
    if len(znappy.host.snapshots) == 0:
        print "No snapshots found on this machine"
        exit(0)

    logger.debug(znappy.host.snapshots)

    table = PrettyTable(fields=["name", "driver", "target", "time"])

    for s in znappy.host.snapshots.values():
        table.add_row([s.name, s.driver, s.target, s.time])

    print table.get_string(sortby=args.get('--sort', 'time'), reversesort=args.get('--reverse', False))


def action_restore(znappy, args):
    logger.debug(znappy.host.snapshots)
    logger.debug(args)

    candidates = filter(lambda s: s.name == args['<name>'], znappy.host.snapshots.values())

    if len(candidates) != 1:
        logger.fatal('Snapshot name `{}` not found or ambiguous')
        exit(1)

    znappy.load_drivers()

    try:
        znappy.execute_event(['pre_restore'])
        znappy.execute_event(['start_restore'])
        znappy.execute_event(['do_restore'], None, snapshot=candidates[0])
    except Exception:
        logger.fatal('Could not restore!')
    finally:
        znappy.execute_event(['end_restore'])
        znappy.execute_event(['post_restore'])


def action_delete(znappy, args):
    try:
        snapshot = znappy.host.snapshots[args['<name>']]
    except KeyError:
        print "Snapshot not found on this host"
        exit(1)

    znappy.load_drivers()
    znappy.execute_event(['delete_snapshot'], znappy.snapshot.driver, snapshot=snapshot)


def main(db, args):
    logger.debug("Using arguments: {0}".format(args))

    module = sys.modules[__name__]

    if not args['--cluster']:
        logger.fatal('No cluster name provided')
        exit(0)

    znappy = Znappy(db, args['--cluster'])

    for c in ['create', 'list', 'restore', 'delete']:
        if args[c] and hasattr(module, 'action_{}'.format(c)):
            command = getattr(module, 'action_{}'.format(c))
            command(znappy, args)

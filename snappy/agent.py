"""
    Usage: snappy-agent [options]

Options:
    -c FILE, --config FILE  Configuration file to use [default: ./config-sample.yaml]
"""

from . import backend, keystore, lockagent, snapshot, utils

from docopt import docopt
import logging
import time
import yaml


def check_update(ks, config):
    snapshots = ks.list_snapshots()

    utils.logger.debug(snapshots)

    last_snapshot = max(snapshots, key=lambda v:v['time'])

    if last_snapshot['time'] < int(time.time() - config['max_age']):
        utils.logger.debug('last snaphot ({0}) is older then {1} seconds'.format(last_snapshot['time'], config['max_age']))
        return True
    else:
        utils.logger.debug('last snapshot ({0}) is ok'.format(last_snapshot['time']))
        return False


def main(args):
    utils.get_logging('snappy-agent')

    utils.logger.debug("Using arguments: {0}".format(args))

    config = utils.get_config(args['--config'])

    utils.logger.debug("Using configuration: {0}".format(config))

    # get keystore and lockagent
    with keystore.get(*config['keystore']) as ks, lockagent.get(*config['lockagent']) as la:
        # preflight check
        if check_update(ks, config['snapshot']) and la.acquire():
            with backend.get(*config['backend']) as be:
                # do the snapshot log
                time.sleep(10)
                pass
        else:
            utils.logger.debug('Preflight failure.. skipping run')

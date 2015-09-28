#!/usr/bin/env python
import consul
import time
import logging
import yaml
import json

from snappy import keystore, lockagent, backend

logging.basicConfig(level=logging.DEBUG)

logger   = logging.getLogger('snappy-agent')


def get_config(filename):
    with open(filename) as f:
        config = yaml.load(f)

    return config


""" Checks if this agent requires a run, it will return True
    if a run is required, otherwise false
"""
def check_update(ks, config):
    snapshots = ks.list_snapshots()

    last_snapshot = max(snapshots, key=lambda v: v['time'])

    if last_snapshot['time'] < int(time.time() - config['snapshot']['max_age']):
        logger.debug('last snapshot ({0}) is older then {1} seconds'.format(last_snapshot['time'], config['snapshot']['max_age']))
        return True
    else:
        logger.debug('last snapshot ({0}) is ok'.format(last_snapshot['time']))
        return False


def main():
    config = get_config('config-sample.yaml')

    with keystore.get(*config['keystore']) as ks, lockagent.get(*config['lockagent']) as la:
        # preflight check
        if check_update(ks, config) and la.acquire():
            # start the snapshot process
            be = backend.get(*config['backend'])

            be.runner()
        else:
            logger.debug('Preflight failure.. skipping run')


if __name__ == "__main__":
    main()


# keystore.add_snapshot(**{"name": "jorn-is-cool"})

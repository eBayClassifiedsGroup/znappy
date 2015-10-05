from . import backend, keystore, lockagent, snapshot, utils

import time


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
    utils.get_logging('snappy-agent', args)

    utils.logger.debug("Using arguments: {0}".format(args))

    config = utils.get_config(args['--config'])

    utils.logger.debug("Using configuration: {0}".format(config))

    # get keystore and lockagent
    with keystore.get(*config['keystore']) as ks, lockagent.get(*config['lockagent']) as la:
        # preflight check
        if check_update(ks, config['snapshot']) and la.acquire():
            try:
                # notify the backend that we are about to start a snapshot
                be = backend.get(*config['backend'])
                be.start_snapshot()

                snap = snapshot.Snapshot(ks, config['snapshot'])
                snap.create()

                utils.logger.debug(snap)

                snap.save()
            except Exception as e:
                utils.logger.debug(e)

                # roll back stuff
            finally:
                # notify the backend we are done making the snapshot
                if be:
                    be.end_snapshot()
        else:
            utils.logger.debug('Preflight failure.. skipping run')

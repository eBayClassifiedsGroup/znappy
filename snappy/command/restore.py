"""
Restore a snapshot back to disk (mount)

Usage:
    snappy restore <name>
"""

from snappy import backend, keystore, lockagent, snapshot
from snappy.utils import config, logger


def main(args):
    logger.debug("Using arguments: {}".format(args))
    logger.debug("Using config: {}".format(config))

    with keystore.get(*config['keystore']) as ks, lockagent.get(*config['lockagent']) as la:
        if not la.acquire():
            logger.fatal('Could not acquire lock!')
            exit(3)

        try:
            snap = snapshot.Snapshot(ks, name=args['<name>'])
            #be = backend.get(*config['backend'])
            
            #be.start_restore()

            # restore the snapshot, fingers crossed
            snap.restore()
        except KeyError:
            logger.info('Snapshot not found in datastore')
        finally:
            #if be:
            #    be.end_restore()
            pass

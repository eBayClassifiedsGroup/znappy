"""

Usage:
        znappy bootstrap [options]

Options:
    -c=<FILE>, --config=<FILE>  Configuration file to use
"""

import logging
import json

logger = logging.getLogger(__name__)


def main(db, args):
    try:
        with open(args['--config']) as f:
            data = json.load(f)

            db.put(
                'service/znappy/clusters/{}/config'.format(args['--cluster']),
                json.dumps(data)
            )
    except:
        logger.fatal('Failed to update configuration!')

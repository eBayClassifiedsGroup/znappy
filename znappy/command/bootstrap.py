"""

Usage:
        znappy bootstrap [options]

Options:
    -c=<FILE>, --config=<FILE>  Configuration file to use
"""

from znappy import utils
import logging
import json

logger = logging.getLogger(__name__)


def main(db, args):
    config = utils.config

    with open(args['--config']) as f:
        bdata = json.load(f)

    logger.debug(bdata)

    cluster = bdata.pop('cluster', 'default')
    
    # create the config key
    db.put(
        'service/znappy/config',
        json.dumps(bdata)
    )

    # create directory for hosts
    db.put(
        'service/znappy/{}/'.format(cluster),
        None
    )

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
        data = json.load(f)

    cluster = os.eviron.get('ZNAPPY_CLUSTER')
    # create the config key
    db.put(
        'service/znappy/clusters/{}/config'.format(cluster),
        json.dumps(data)
    )

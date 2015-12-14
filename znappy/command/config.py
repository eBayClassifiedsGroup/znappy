"""

Usage:
    znappy config get
    znappy config update (--config=<FILE>)

Options:
    -c=<FILE>, --config=<FILE>  YAML configuration file to use
"""

from znappy import Znappy
import logging
import json
import sys
import yaml

logger = logging.getLogger(__name__)


def action_get(znappy, db, args):
    print yaml.safe_dump(znappy.cluster.config)


def action_update(znappy, db, args):
    # open the new config file
    try:
        with open(args['--config']) as f:
            new_config = yaml.load(f)
    except:
        logger.fatal('Failed to open configuration file!')
        exit(1)

    old_config = znappy.config

    if not new_config.get('config-version'):
        logger.fatal('New configuration does not have a version, ignoring update')
        exit(1)

    if new_config['config-version'] > old_config.get('config-version', 1):
        logger.info("Newer config version: (new:{}, old:{})".format(
            new_config['config-version'],
            old_config.get('config-version', 1)
        ))

        # update the config in consul
        db.put(
            'service/znappy/clusters/{}/config'.format(args['--cluster']),
            json.dumps(new_config, indent=2)
        )
    else:
        logger.info("Configuration version not newer, ignoring..")


def main(db, args):
    module = sys.modules[__name__]
    znappy = Znappy(db, args['--cluster'])

    for command in ['get', 'update']:
        funcname = 'action_{}'.format(command)

        if args[command] and hasattr(module, funcname):
            action = getattr(module, funcname)
            result = action(znappy, db, args)

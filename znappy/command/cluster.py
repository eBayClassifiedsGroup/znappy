"""
Usage:
    znappy cluster list [options]
    znappy cluster restore [options]
    znappy cluster --help

Options:
    --minute=<m>        Minutes ago to search for [default: 15]
"""

from znappy import Znappy
import time
import sys
import getpass
from datetime import datetime, timedelta
from prettytable import PrettyTable
from fabric.api import env, sudo, task

env.warn_only = True


@task
def tsudo(target, cmd):
    env.host_string = target
    return sudo(cmd)


def list_snapshots(cluster, t):
    snapshots = {}

    for h in cluster.hosts:
        snapshots_before_time = filter(lambda s: s.time < (t - 3), cluster.hosts[h].snapshots)
        if snapshots_before_time:
            snapshot = sorted(snapshots_before_time, key=lambda s: s.time, reverse=True)[0]

        snapshots[h] = snapshot

    return snapshots


def snapshot_table(snapshots, t=int(time.time())):
    table = PrettyTable(fields=['host', 'snapshot', 'time', 'lag'])
    for host in snapshots:
        snapshot = snapshots[host]
        lag      = int(t - snapshot.time)

        table.add_row([host, snapshot.name, snapshot.time, lag])

    return table


def action_list(znappy, t):
    snapshots = list_snapshots(znappy.cluster, t)

    table = snapshot_table(snapshots, t)

    return table.get_string(sortby='host')


def action_restore(znappy, t):
    snapshots = list_snapshots(znappy.cluster, t)

    print snapshot_table(snapshots, t).get_string(sortby='host')

    master = sorted(snapshots.values(), key=lambda s: s.time, reverse=True)[0].host

    print "Master host will be: {}".format(master.name)

    choice = raw_input("Are you sure? [y/N]: ").lower()
    if choice != 'y':
        return "Aborted!"

    env.user     = raw_input('[ldap] username: ')
    env.password = getpass.getpass('[sudo] password for {}: '.format(env.user))

    if not tsudo('localhost', '/bin/true'):
        return 'Failed to verify credentials'

    # lock the whole cluster
    while not znappy.cluster.lock():
        time.sleep(0.5)

    # time to bork the sjit
    try:
        znappy.load_drivers()
        znappy.execute_event(['pre_cluster_restore'])
        for snapshot in snapshots:
            env.host_string = snapshot.host
            sudo('znappy snapshot restore {0}'.format(snapshot.name))
        znappy.execute_event(['post_cluster_restore'], master_host=master)
    # ye it should be that easy
    except Exception, e:
        print e.message
        return "Failed to restore cluster!"

    znappy.cluster.release()

    return "cluster restore complete"


def main(db, args):
    module = sys.modules[__name__]
    znappy = Znappy(db, args['--cluster'])

    time_before = datetime.today() - timedelta(minutes=int(args['--minute']))
    time_before = int(time.mktime(time_before.timetuple()))
    result      = "Action not found!"

    for c in ['list', 'restore']:
        funcname = 'action_{}'.format(c)
        if args[c] and hasattr(module, funcname):
            action = getattr(module, funcname)
            result = action(znappy, time_before)

    print result

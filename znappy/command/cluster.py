"""
List or restore cluster wide snapshots. Prompts for a timestring
to find closest match snapshots.

Usage:
    znappy cluster list
    znappy cluster restore
"""

from znappy import Znappy
import time
import sys
import getpass
from datetime import datetime
from prettytable import PrettyTable
from fabric.api import env, sudo, task

env.warn_only = True


@task
def tsudo(target, cmd):
    env.host_string = target
    return sudo(cmd)


def list_snapshots(cluster, t):
    snapshots = {}

    def sort_by_time(s):
        return sorted(s, key=lambda s: s.time)

    for h in cluster.hosts:
        host_snapshots = cluster.hosts[h].snapshots.values()
        if not host_snapshots:
            continue
        snapshots_before_time = filter(lambda s: s.time < (t - 3), host_snapshots)
        if snapshots_before_time:
            # Grab the closest snapshot older than specified time
            snapshot = sort_by_time(snapshots_before_time)[-1]
        else:
            # No snapshot older than specified time so grab the oldest
            snapshot = sort_by_time(host_snapshots)[0]

        snapshots[h] = snapshot

    return snapshots


def snapshot_table(snapshots, t=int(time.time())):
    table = PrettyTable(fields=['host', 'snapshot', 'time', 'lag'])
    for host in snapshots:
        snapshot  = snapshots[host]
        lag       = abs(int(t - snapshot.time))
        timestamp = datetime.fromtimestamp(int(snapshot.time)).strftime('%Y-%m-%d %H:%M:%S')

        table.add_row([host, snapshot.name, timestamp, lag])

    return table


def action_list(znappy, t):
    snapshots = list_snapshots(znappy.cluster, t)

    table = snapshot_table(snapshots, t)

    return table.get_string(sortby='host')


def action_restore(znappy, t):
    snapshots = list_snapshots(znappy.cluster, t)

    print snapshot_table(snapshots, t).get_string(sortby='host')

    # Sort by smallest amount of (absolute) "lag" to get closest snapshot
    master = sorted(snapshots.values(), key=lambda s: abs(int(t - s.time)))[0].host

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


def get_user_time():
    while True:
        try:
            time_input = raw_input('[YYYY-MM-DD HH:mm:ss]: ')
            parse_time = datetime.strptime(time_input, "%Y-%m-%d %H:%M:%S").timetuple()
            return int(time.mktime(parse_time))
        except Exception:
            print("Invalid time format given..")
            continue


def main(db, args):
    module = sys.modules[__name__]
    znappy = Znappy(db, args['--cluster'])

    print("Specify a point in time to look for closest matching snapshots, e.g. 2016-01-31 11:55:55")
    time_before = get_user_time()

    result      = "Action not found!"

    for c in ['list', 'restore']:
        funcname = 'action_{}'.format(c)
        if args[c] and hasattr(module, funcname):
            action = getattr(module, funcname)
            result = action(znappy, time_before)

    print result

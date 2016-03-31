# Znappy

Znappy is a distributed, decentralized agent for creating snapshots.

The currenty drivers are based on ZFS and MySQL, using ZFS to create snapshots of the MySQL data. However,
due to it's modulair/event-driven design, this can easily be extended.

Znappy works by storing information in Consul and using the locking mechanism provided by Consul to desync
the creation of states/snapshots.


## Installation

For debian, znappy package can be build using:

```shell
fpm \
    -s python \
    -t deb \
    --no-python-fix-dependencies \
    --no-python-fix-name \
    --deb-upstart  etc/init/znappy-daemon \
    setup.py
```

## Configuration

For the agent to work, it expects 2 configuration files in `/etc/znappy`

The `cluster.yaml` file will not be used by the agent, but is needed in the console to create the cluster
configuration in Consul, this can be triggered by `znappy config update` (see `znappy config --help`)

### client.yaml
```yaml
---
cluster: foo
log-level: WARN
consul-host: localhost
consul-port: 8500
```

and..

### cluster.yaml
```yaml
---
config-version: 1
drivers:
  znappy.backend.mysql:
    mycnf_path: /root/.my.cnf
    mycnf_section: client
    failover_creds: /etc/mysql/failover.cnf
  znappy.snapshot.zfs:
    filesystem: data/mysql
    # Dataset properties to copy when recoverying snapshots
    properties:
      - atime
      - compression
      - mountpoint
      - primarycache
      - recordsize
      - sync
  znappy.monitor.snapshot: {}
snapshot:
  min_age: 900
  rotate: 48
```

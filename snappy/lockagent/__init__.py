from snappy.lockagent.consul import ConsulLockAgent
import importlib


__all__ = ["consul"]


def get(classname, *args, **kwargs):
    m = importlib.import_module('snappy.lockagent')
    return getattr(m, classname)(*args, **kwargs)

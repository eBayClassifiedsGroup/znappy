from snappy.keystore.consul import ConsulKeystore
import importlib


__all__ = ["consul"]


def get(classname, *args, **kwargs):
    m = importlib.import_module('snappy.keystore')
    return getattr(m, classname)(*args, **kwargs)

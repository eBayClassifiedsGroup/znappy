from snappy.storage.git import GITStorage
import importlib


__all__ = ["git"]


def get(classname, *args, **kwargs):
    m = importlib.import_module('snappy.storage')
    return getattr(m, classname)(*args, **kwargs)

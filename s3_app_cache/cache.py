import gzip
import os
import pickle
import sys
import tempfile
from typing import Any, List
from functools import wraps
import json
import hashlib
import inspect
import pprint
import jsonpickle
import jsonpickle.ext.numpy as jsonpickle_numpy
import jsonpickle.ext.pandas as jsonpickle_pandas

jsonpickle_numpy.register_handlers()
jsonpickle_pandas.register_handlers()
cache_location = "a/s3/path"


def cache_wrapper(func):
    @wraps(func)
    def inner(*args, **kwargs):
        invalidate_cache = kwargs.pop("invalidate_cache", False)

        # get names and default values of func
        fullargspec = inspect.getfullargspec(func)

        is_instance_method = False
        if fullargspec.args and "self" in fullargspec.args:
            is_instance_method = True

        # Stringify everything, hash it, and cache it
        label = ""
        if is_instance_method:
            class_name = args[0].__class__.__name__
            label += f"{class_name}."

        func_name = func.__name__
        label += func_name
        s = jsonpickle.encode(
            (label, fullargspec, args, kwargs),
            unpicklable=False,
            warn=True,
            keys=True,
            make_refs=False,
        )

        with open("/mnt/jp_debug.txt", "w") as f:
            print(label + " Total args:\n" + pprint.pformat(json.loads(s)), file=f)

        sha = hashlib.sha256(s.encode("utf-8")).hexdigest()
        cache = check_cache(sha, cache_location)
        if cache is not None and not invalidate_cache:
            print(f"Returning cached value from {cache_location+sha}")
            return cache
        value = func(*args, **kwargs)
        print(f"Caching result to {cache_location + sha}")
        cache_object(value, cache_location + sha)
        return value

    return inner


def check_cache(sha: str, _cache_location: str):
    return None


def cache_object(val: Any, s3_path: str):
    return None


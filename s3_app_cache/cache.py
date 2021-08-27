import gzip
import os
import pickle
import tempfile
from typing import Any
from functools import wraps
import hashlib
import inspect
import boto3

s3_address = os.environ.get("S3_APP_CACHE_PATH")
boto_profile = os.environ.get("S3_APP_CACHE_PROFILE", "default")

BUCKET_NAME = s3_address.split("/")[2]
CACHE_LOCATION = "/".join(s3_address.split("/")[3:])
assert (
    CACHE_LOCATION
), "S3_APP_CACHE_PATH env var should be of form s3://<bucket-name>/a/path/to/cache/"

session = boto3.session.Session(profile_name=boto_profile)
s3 = session.client("s3")


def is_valid_cache_location(_path):
    return _path is not None


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
        s = pickle.dumps((label, fullargspec, args, kwargs))

        # with open("/mnt/jp_debug.txt", "w") as f:
        #     print(label + " Total args:\n" + pprint.pformat(json.loads(s)), file=f)

        sha = hashlib.sha256(s).hexdigest()
        cache = check_cache(sha, CACHE_LOCATION)
        if cache is not None and not invalidate_cache:
            print(f"Returning cached value from {CACHE_LOCATION+sha}")
            return cache
        value = func(*args, **kwargs)
        print(f"Caching result to {CACHE_LOCATION + sha}")
        cache_object(value, CACHE_LOCATION + sha)
        return value

    return inner


def check_cache(sha: str, _cache_path: str):
    if not _cache_path:
        return None
    with tempfile.TemporaryDirectory() as tmpdir_path:
        try:
            data_path = _cache_path + sha
            local_path = tmpdir_path + "/" + os.path.basename(data_path)
            download_s3_object_to_local_file(data_path, local_path)
            with gzip.open(local_path) as f:
                data = pickle.load(f)
        except Exception:  # catch any issue as cache miss
            return None
    return data


def cache_object(_data: Any, s3_path: str):
    with tempfile.TemporaryDirectory() as tmpdir_path:
        local_path = tmpdir_path + os.path.basename(s3_path)
        with gzip.GzipFile(local_path, "wb") as f:
            pickle.dump(_data, f)
        upload_local_file_to_s3(local_path, s3_path)


def download_s3_object_to_local_file(s3_path, local_path):
    s3.download_file(BUCKET_NAME, s3_path, local_path)


def upload_local_file_to_s3(local_path, s3_path):
    with open(local_path, "rb") as f:
        s3.upload_fileobj(f, BUCKET_NAME, s3_path)

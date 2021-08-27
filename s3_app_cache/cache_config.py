import boto3
import os


class CacheConfig(object):
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(CacheConfig, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self.s3_address = os.environ.get("S3_APP_CACHE_PATH")
        self.boto_profile = os.environ.get("S3_APP_CACHE_PROFILE", "default")
        self.bucket_name = self.s3_address.split("/")[2]
        self.cache_location = "/".join(self.s3_address.split("/")[3:])
        assert (
            self.cache_location
        ), "S3_APP_CACHE_PATH env var should be of form s3://<bucket-name>/a/path/to/cache/"

        self._initialized = True
        print("Done INIT")

    def set_cache_location(self, path: str):
        self.cache_location = path

    def set_bucket_name(self, bucket_name: str):
        self.bucket_name = bucket_name

    def set_boto_profile(self, profile_name):
        self.boto_profile = profile_name

    def get_s3_client(self):
        session = boto3.session.Session(profile_name=self.boto_profile)
        s3 = session.client("s3")
        return s3

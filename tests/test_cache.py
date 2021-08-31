import boto3
from moto import mock_s3
import pytest
import pandas as pd
from s3_app_cache.cache import cache_wrapper
from s3_app_cache.cache_config import CacheConfig


@cache_wrapper
def func_to_cache(df):
    """Mock function to test caching"""
    return df


def _obj_dict_to_df(obj_dict):
    """Turns `list_objects_v2` api response into a dataframe"""
    return pd.DataFrame.from_records(obj_dict["Contents"])[
        ["Key", "LastModified", "Size"]
    ]


@pytest.fixture
def df():
    return pd.DataFrame({"test": [1, 2, 3]})


@pytest.fixture
def config():
    """Config singleton initialized with moto s3 client"""
    with mock_s3():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="cache")
        config = CacheConfig(s3_address="s3://cache/folder",)
        config.set_s3_client(s3)
        yield config


def test_basic_config(config):
    assert config.bucket_name == "cache", "bucket_name not properly set"
    assert config.cache_location == "folder", "cache_location not properly set"
    assert config.s3_address == "s3://cache/folder", "s3_address not properly set"


def test_cache_pandas_dataframe_to_s3(config, df):
    func_to_cache(df)
    client = config.get_s3_client()
    res = _obj_dict_to_df(client.list_objects_v2(Bucket="cache"))
    assert len(res) == 1, "Pandas df did not cache to s3"
    func_to_cache(df)
    res = _obj_dict_to_df(client.list_objects_v2(Bucket="cache"))
    assert len(res) == 1, f"df was cached {len(res)} times (should be 1)"


def test_retrieve_pandas_dataframe_from_cache(config, df):
    df1 = func_to_cache(df)
    df2 = func_to_cache(df)
    assert df.equals(df2), "Different dataframe retrieved from cache"
    assert df1.equals(df2), "Different dataframe retrieved from cache"

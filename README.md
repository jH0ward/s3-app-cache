# s3-app-cache

You must have an S3 backend configured in a manner that works with boto3. An easy way to do this is to create a `~/.aws/config` file

```
poetry config virtualenvs.in-project true
poetry install
source .venv/bin/activate
```

# TODO
1. Logic to load a config for customizable behavior
2. s3_util code (to_s3, read_s3)
3. cache_object, check_cache, delete_cache

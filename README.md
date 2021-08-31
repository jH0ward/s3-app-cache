# s3-app-cache

## Usage

You must have an S3 backend configured in a manner that works with boto3. An easy way to do this is to create a `~/.aws/config` file
## Developer Setup

```
poetry config virtualenvs.in-project true
poetry install
source .venv/bin/activate
```

## Testing

```
source .venv/bin/activate
python -m pytest
```

## TODO
1. Ability to group cached objects by job etc.
2. `list_cache()`
3. `check_cache(group=)`
4. `delete_cache(group=)`

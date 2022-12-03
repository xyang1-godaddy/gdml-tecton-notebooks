import datetime
import logging
import os
from functools import lru_cache

import boto3
import lz4.block
import ujson
from botocore.exceptions import ClientError

from shopperml_feature_store.shard import format_key_prefix
from shopperml_feature_store.shard import get_s3_shopper_path
from shopperml_feature_store.utils.retry_loop import retry_loop

RETRYABLE_S3_ERRORS = ("ServiceUnavailable", "SlowDown", "InternalError")

SEQUENCE_FEATURES = [
    "domaininfo",
    "products2",
    "uds_order_last90",
    "uds_order_lifetime",
    "uds_product_billing",
    "uds_traffic_last10",
]
NON_SEQUENCE_FEATURES = ["marketing_cdl"]
DEFAULT_LOOK_BACK_DAYS = 30

# These snapshots types are "END OF LIFE"'d and not available for newer data. Fetching will fail on older data if
# metadata is mixed. Code below uses this list to ignore requests for deprecated features.
EOL_SNAPSHOT_TYPES = {"business_model", "productivity_addons", "shopper_nps", "shopper360", "pltv_order_feature"}

log = logging.getLogger("s3index")


def s3_config():
    """Return boto3 `Client` or `Resource` arguments for s3"""
    return {"endpoint_url": os.environ.get("AWS_S3_ENDPOINT_URL", None)}


def get_data_s3_bucket_name():
    ssm_client = boto3.client("ssm", region_name="us-west-2")
    env = ssm_client.get_parameter(Name="/AdminParams/Team/Environment")["Parameter"]["Value"]
    return f"gd-gxcoreservices-{env}-shopperml-data"


def get_s3_object_name(prefix, key):
    return "{}/{}".format(prefix, key)


def is_sequence_type(snapshot_type):
    """
    A function that returns a boolean when given the snapshot type to indicate
        whether the snapshot is a sequential type or not.
    If the snapshot_type is sequential, it could be a list of len=0,1,...,N;
    if the snapshot_type is not sequential, it could be a list of len=0,1 (either empty or one element),
    """
    if snapshot_type in SEQUENCE_FEATURES:
        return True
    elif snapshot_type in NON_SEQUENCE_FEATURES:
        return False
    else:
        raise KeyError("No record found for {} about whether it is a sequence type or not".format(snapshot_type))


def decompress(data):
    return lz4.block.decompress(data)


def compress(data, mode="default"):
    # lz4.block.compress takes bytes. Add encoding for py2/3 compatibility
    data = data.encode("utf-8") if not isinstance(data, bytes) else data
    return lz4.block.compress(data, mode=mode)


@retry_loop(retryable_errors=RETRYABLE_S3_ERRORS)
def read_s3_json_object(s3_client, bucket_name, key, attempts=100):
    json_object = s3_client.Object(bucket_name, key)
    try:
        response = json_object.get()
        value = response["Body"].read()
        content_type = response["ContentType"]
        # Assume 'binary/octet-stream' is lz4 compressed.
        if content_type in ("binary/octet-stream", "application/x-lz4"):
            # Historically, we used 'binary/octet-stream' for everything, even
            # uncompressed data (e.g., metadata.json). So we handle that case
            # here.
            try:
                value = decompress(value)
            except lz4.block.LZ4BlockError:
                pass
        # Assume everything else is JSON (i.e., 'application/json')
        return ujson.loads(value)
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return {}
        raise RuntimeError(f"Failure reading S3 object s3://{bucket_name}/{key}") from e


read_metadata = lru_cache(256)(read_s3_json_object)
read_shopper_data = lru_cache(1)(read_s3_json_object)


class S3Reader(object):
    """
    Facilitates querying for a shopper given list of snapshot types and collect date
    NOTE: This class is NOT thread-safe. In multi-threaded scenarios, use one instance per-thread.

    Returns an dict containing the shopper data.
    """

    def __init__(self, snapshot_types=None, cache_directory=None, look_back_days=DEFAULT_LOOK_BACK_DAYS):
        """
        :param snapshot_types: a list of snapshot types which the reader will query for
                               if snapshot_types is None, it will query from all available snapshots
        :param cache_directory: path to directory to cache S3 objects in
        :param look_back_days: int, the furthest number of days when looking back for the last successful snapshot
            prior to the collect_date. if set to 0, no look back is enabled.
        """
        super(S3Reader, self).__init__()
        self.snapshot_types = snapshot_types
        self.cache_directory = cache_directory
        self.look_back_days = look_back_days
        self.s3_client = boto3.resource("s3", **s3_config())
        self.s3_bucket_name = get_data_s3_bucket_name()

    def _get_cache_path(self, key, collect_date):
        s3_key_prefix = format_key_prefix(collect_date)
        cache_path = os.path.join(self.cache_directory, self.s3_bucket_name, get_s3_object_name(s3_key_prefix, key))
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        return cache_path

    def _get_cached(self, key, collect_date):
        if not self.cache_directory:
            raise KeyError(f"{key} missed (S3 cache not enabled)")
        try:
            with open(self._get_cache_path(key, collect_date), "rb") as cache_file:
                return ujson.loads(decompress(cache_file.read()))
        except FileNotFoundError:
            raise KeyError(f"{key} missed (not found)")
        except ValueError:
            raise KeyError(f"{key} missed (maps to invalid object)")

    def _set_cached(self, key, collect_date, cache_object):
        if not self.cache_directory:
            return
        try:
            with open(self._get_cache_path(key, collect_date), "wb") as cache_file:
                cache_file.write(compress(ujson.dumps(cache_object)))
        except Exception as error:
            log.warning("failed to cache %s: %s", key, error)

    def _get(self, shopper_id, collect_date):
        try:
            return self._get_cached(shopper_id, collect_date)
        except KeyError:
            pass
        result = read_shopper_data(self.s3_client, self.s3_bucket_name, get_s3_shopper_path(shopper_id, collect_date))
        self._set_cached(shopper_id, collect_date, result)
        return result

    def _get_metadata(self, collect_date):
        try:
            return self._get_cached("metadata.json", collect_date)
        except KeyError:
            pass
        s3_key_prefix = format_key_prefix(collect_date)
        result = read_metadata(self.s3_client, self.s3_bucket_name, "{}/metadata.json".format(s3_key_prefix))
        self._set_cached("metadata.json", collect_date, result)
        return result

    def lookup(self, shopper_id, collect_date):
        """
        Formats and filters by snapshot_type the deserialized raw output s3 returned on the given shopper_id.
        Returns an empty dict if the shopper isn't found, the requested snapshot types are not found, or when
        snapshot_types is empty.
        """
        collect_date = self.check_latest_available_snapshot(collect_date)
        if collect_date is None:
            log.warning("Failed to find a S3 snapshot within {} day of {}".format(self.look_back_days, collect_date))
            return {}  # TODO: change if we want to change the default return or raise an error

        values = self._get(shopper_id, collect_date)
        metadata = self._get_metadata(collect_date)
        if values is None or not metadata:
            return {}

        snapshot_types = self.snapshot_types
        if snapshot_types is None:
            snapshot_types = [name for name, snapshot_info in values.items() if snapshot_info]

        # Remove/ignore deprecated snapshot types
        snapshot_types = set(snapshot_types) - EOL_SNAPSHOT_TYPES

        results = {}

        for snapshot_type in snapshot_types:
            fieldnames = [name.lower() for name in metadata["fieldnames"][snapshot_type]]
            if values.get(snapshot_type) is not None:
                results[snapshot_type] = [dict(zip(fieldnames, value)) for value in values[snapshot_type]]

        return results

    @lru_cache(maxsize=10000)
    def check_latest_available_snapshot(self, collect_date):
        for days_back in range(self.look_back_days):
            date = collect_date - datetime.timedelta(days_back)
            result = self._get_metadata(date)
            if result:
                return date

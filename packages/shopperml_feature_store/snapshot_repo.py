import json
import logging
import os
from functools import lru_cache

from shopperml_feature_store.s3_reader import S3Reader
from shopperml_feature_store.s3_reader import is_sequence_type

log = logging.getLogger(__name__)

_SNAPSHOT_FIELDNAMES_FILE = "snapshot_fieldnames.json"


@lru_cache(1)
def get_snapshot_fieldnames(filename):
    """Get the JSON object with all snapshot_types and fieldnames available to this dataset"""
    if filename is None:
        filename = _SNAPSHOT_FIELDNAMES_FILE
    with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), filename)) as f:
        info = json.load(f)
    log.debug("Available snapshots and associated fieldnames:\n{}".format(info))
    return info


class SnapshotRepo(object):
    """
    Access shopper snapshots (indexed features) created by the ShopperML data pipeline (stored in S3).
    NOTE: This class is NOT thread-safe. In multi-threaded scenarios, use one instance per-thread.
    """

    def __init__(self, use_s3=True, cache_directory=None, fieldname_file=None):
        super(SnapshotRepo, self).__init__()
        self.use_s3 = use_s3
        self.cache_directory = cache_directory
        self.s3_reader = S3Reader(cache_directory=self.cache_directory)
        self.snapshot_fieldnames = get_snapshot_fieldnames(fieldname_file)

    def get_raw(self, key, date, feature):
        """get the data in S3 snapshots based on key (shopper_id), date (snpashot date) and feature name"""
        indexed_features = self.s3_reader.lookup(key, date) or dict()
        return indexed_features.get(feature, [])

    def get(self, key, date, feature):
        return self.get_flattened(key, date, feature)

    def get_flattened(self, key, date, feature):
        """return a dict with flattened snapshot: {feature.fieldname: a list or a value}"""
        return self._flatten(self.get_raw(key, date, feature), feature)

    def get_flattened_columns(self, feature):
        """get the column names associated to the feature as a list"""
        return [f"{feature}.{fieldname}" for fieldname in self.snapshot_fieldnames[feature]]

    def _flatten(self, raw, feature):
        """
        Convert a LIST(DICT()) to DICT(LIST())
        :param raw: a list of dict, all dicts should share the same keys
        :param feature: snapshot name, used as a prefix
        :return: a dict, {[feature].[fieldname]: a list or a single value}
        """
        result = dict()
        fieldnames = self.snapshot_fieldnames.get(feature)
        for fieldname in fieldnames:
            # sequence feature: return a list for each fieldname
            if is_sequence_type(feature):
                result[f"{feature}.{fieldname}"] = [record.get(fieldname) for record in raw] if raw else []
            # non-sequence feature: remove the outer list, return a value or None for each fieldname
            else:
                result[f"{feature}.{fieldname}"] = raw[0].get(fieldname) if raw else None
        return result

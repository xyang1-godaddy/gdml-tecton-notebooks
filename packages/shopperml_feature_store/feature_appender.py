import os

import pyarrow.dataset as ds

import shopperml_feature_store.feature_schema as feature_schema
from shopperml_feature_store.snapshot_repo import SnapshotRepo
from shopperml_feature_store.utils.isodate import parse


class FeatureAppender:
    default_cache_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), ".shopperml-feature-store")
    default_output_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "dataset")

    def __init__(
        self,
        input_path,
        output_path=None,
        feature_groups=feature_schema.DEFINED_FEATURE_GROUPS,
        shopper_id_col="shopper_id",
        timestamp_col="order_ts",
    ):
        """
        Takes feature file, appends features from s3 'snapshot' data

        :param feature_groups: features groups to be added
        :param input_path: Dir to read from
        :param output_path: Dir to write to
        :param shopper_id_col: name of column to use as shopper_id
        :param timestamp_col: name of column to use as timestamp
        """

        self.input_path = input_path
        self.output_path = output_path
        self.feature_groups = feature_groups
        self.shopper_id_col = shopper_id_col
        self.timestamp_col = timestamp_col
        self.snapshot_repo = SnapshotRepo(cache_directory=self.get_cache_path())

    def append(self):
        if not set(self.feature_groups).issubset(feature_schema.DEFINED_FEATURE_GROUPS):
            raise Exception(f"Invalid Feature Groups. Valid values are {feature_schema.DEFINED_FEATURE_GROUPS}.")

        self.make_cache_path()
        self.make_output_path()

        dataset = ds.dataset(self.input_path)
        for count, record_batch in enumerate(dataset.to_batches()):
            df = record_batch.to_pandas()
            self.initialize_new_columns(df)
            self.append_features_to_dataframe(df)
            self.apply_schema_to_appended_features(df)
            self.write_dataframe_to_file(df, count)

    def get_cache_path(self):
        return self.default_cache_path

    def make_cache_path(self):
        os.makedirs(self.default_cache_path, exist_ok=True)

    def make_output_path(self):
        os.makedirs(self.get_output_path(), exist_ok=True)

    def get_output_path(self):
        if self.output_path is not None:
            return self.output_path
        else:
            return self.default_output_path

    def initialize_new_columns(self, df):
        feature_schema.initialize_columns(df, self.feature_groups)

    def append_features_to_dataframe(self, df):
        for index, row in df.iterrows():
            try:
                s3_key = getattr(row, self.shopper_id_col)
            except AttributeError:
                raise Exception(f"Invalid Shopper ID Column '{self.timestamp_col}', must be string.")

            try:
                s3_date = parse(getattr(row, self.timestamp_col))
            except AttributeError:
                raise Exception(f"Invalid Timestamp Column '{self.timestamp_col}', must be date object.")

            schema_defined_fields = feature_schema.all_fields()

            for feature_group in self.feature_groups:
                snapshot = self.snapshot_repo.get(s3_key, s3_date, feature_group)
                for key, value in snapshot.items():
                    if key in schema_defined_fields:
                        df.at[index, key] = value

    def apply_schema_to_appended_features(self, df):
        feature_schema.apply(df)

    def write_dataframe_to_file(self, df, count):
        parquet_filename = f"{self.get_output_path()}/dataset_{count:05d}.parquet"
        df.to_parquet(parquet_filename)

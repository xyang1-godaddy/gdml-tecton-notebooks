class BaseDataset(object):

    indexed_features = []

    def __init__(self):
        super(BaseDataset, self).__init__()
        self.dataset = None
        self.metadata = None

    def create_common_metadata(self, schema, last_update_ts_column, unique_key):
        if self.dataset is not None:
            metadata = {
                "columns": list(self.dataset.columns),
                "dtypes": dict(self.dataset.dtypes),
                "last_update_timestamp": max(self.dataset[last_update_ts_column]),
                "row_count": self.dataset.shape[0],
                "schema": schema,
                "unique_count": self.dataset.groupby(unique_key).count().shape[0],
            }
        else:
            metadata = dict()
        return metadata

    def get_dataset(self):
        return self.dataset

    def get_metadata(self):
        return self.metadata

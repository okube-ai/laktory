from datetime import datetime

import narwhals as nw

import laktory as lk


@lk.api.register_anyframe_namespace("lake")
class LakeNamespace:
    def __init__(self, _df):
        self._df = _df

    def with_last_modified(self):
        return self._df.with_columns(last_modified=nw.lit(datetime.now()))

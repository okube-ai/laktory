import os

from .stockprices import df_brz
from .stockprices import df_meta
from .stockprices import df_meta_polars
from .stockprices import df_slv
from .stockprices import df_slv_polars
from .stockprices import df_slv_stream
from .stockprices import spark


class Paths:
    def __init__(self, file):
        self.root = os.path.dirname(file)
        self.data = os.path.join(self.root, "data")
        self.tmp = os.path.join(self.root, "tmp")

        if not os.path.exists(self.tmp):
            os.makedirs(self.tmp)


class MonkeyPatch:
    def __init__(self):
        self.env0 = {}

    def setenv(self, key, value):
        self.env0[key] = os.getenv(key, None)
        os.environ[key] = value

    def cleanup(self):
        for k, v in self.env0.items():
            if v is None:
                del os.environ[k]
            else:
                os.environ[k] = v

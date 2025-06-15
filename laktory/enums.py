from enum import Enum
from enum import auto
from typing import Any

import narwhals as nw


class DataFrameBackends(str, Enum):
    def _generate_next_value_(name, start, count, last_values):
        return name

    """DataFrame backends (pandas, Polars, PyArrow, ...)."""

    # PANDAS = auto()
    # """Pandas backend."""
    # MODIN = auto()
    # """Modin backend."""
    # CUDF = auto()
    # """cuDF backend."""
    # PYARROW = auto()
    # """PyArrow backend."""
    PYSPARK = auto()
    """PySpark backend."""
    POLARS = auto()
    """Polars backend."""
    # DASK = auto()
    # """Dask backend."""
    # DUCKDB = auto()
    # """DuckDB backend."""
    # IBIS = auto()
    # """Ibis backend."""
    # SQLFRAME = auto()
    # """SQLFrame backend."""
    #
    # UNKNOWN = auto()
    # """Unknown backend."""

    @classmethod
    def from_nw_implementation(
        cls, implementation: nw.Implementation
    ) -> "DataFrameBackends":  # pragma: no cover
        """
        Instantiate DataFrameBackends object from a narwhals implementation.

        Parameters
        ----------
        implementation:
            Narwhals implementation

        Returns
        -------
        output:
            DataFrameBackend
        """
        mapping = {
            nw.Implementation.PYSPARK: DataFrameBackends.PYSPARK,
            nw.Implementation.PYSPARK_CONNECT: DataFrameBackends.PYSPARK,
            nw.Implementation.POLARS: DataFrameBackends.POLARS,
            # get_modin(): Implementation.MODIN,
            # get_cudf(): Implementation.CUDF,
            # get_pyarrow(): Implementation.PYARROW,
            # get_pyspark_sql(): Implementation.PYSPARK,
            # get_polars(): Implementation.POLARS,
            # get_dask_dataframe(): Implementation.DASK,
            # get_duckdb(): Implementation.DUCKDB,
            # get_ibis(): Implementation.IBIS,
            # get_sqlframe(): Implementation.SQLFRAME,
        }

        if implementation not in mapping:
            raise ValueError(f"Implementation {implementation} is not supported.")

        return mapping.get(implementation)

    @classmethod
    def from_df(cls, df: Any) -> "DataFrameBackends":  # pragma: no cover
        """
        Instantiate DataFrameBackends object from a DataFrame.

        Parameters
        ----------
        df:
            DataFrame

        Returns
        -------
        output:
            DataFrameBackend
        """

        if not isinstance(df, (nw.DataFrame, nw.LazyFrame)):
            df = nw.from_native(df)

        return cls.from_nw_implementation(df.implementation)


STREAMING_BACKENDS = [DataFrameBackends.PYSPARK]

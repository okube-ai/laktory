import pandas as pd
import pytest

import laktory
from laktory import models

spark = laktory.get_spark_session()


# --------------------------------------------------------------------------- #
# Custom write functions (must be module-level for importlib resolution)      #
# --------------------------------------------------------------------------- #

# Mutable target path set by each test before calling sink.write()
_write_path: str = None


def _dummy_write(df) -> None:
    pass


def _append_write(df) -> None:
    df = df.to_native()
    df.write.format("DELTA").mode("APPEND").save(_write_path)


# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #


def _build_source_df():
    return spark.createDataFrame(
        pd.DataFrame(
            [
                {"symbol": "S0", "close": 0.42, "open": 0.16},
                {"symbol": "S1", "close": 0.50, "open": 0.97},
                {"symbol": "S2", "close": 0.47, "open": 0.60},
            ]
        )
    )


# --------------------------------------------------------------------------- #
# Model Validation                                                            #
# --------------------------------------------------------------------------- #


def test_write_func_model():
    sink = models.FileDataSink(
        path="./my_table/",
        format="DELTA",
        write_func="tests.datasinks.test_writefunc._dummy_write",
    )
    assert sink.write_func == "tests.datasinks.test_writefunc._dummy_write"
    assert sink.mode is None


def test_write_func_and_mode_exclusive():
    with pytest.raises(ValueError, match="mutually exclusive"):
        models.FileDataSink(
            path="./my_table/",
            format="DELTA",
            mode="APPEND",
            write_func="tests.datasinks.test_writefunc._dummy_write",
        )


def test_write_func_and_merge_cdc_options_exclusive():
    with pytest.raises(ValueError, match="mutually exclusive"):
        models.FileDataSink(
            path="./my_table/",
            format="DELTA",
            write_func="tests.datasinks.test_writefunc._dummy_write",
            merge_cdc_options=models.DataSinkMergeCDCOptions(
                primary_keys=["symbol"],
            ),
        )


# --------------------------------------------------------------------------- #
# Function Resolution                                                         #
# --------------------------------------------------------------------------- #


def test_resolve_write_func():
    resolved = models.FileDataSink._resolve_write_func(
        "tests.datasinks.test_writefunc._dummy_write"
    )
    assert resolved is _dummy_write


def test_resolve_write_func_missing_module():
    with pytest.raises(ModuleNotFoundError):
        models.FileDataSink._resolve_write_func("nonexistent.module.my_func")


def test_resolve_write_func_missing_attr():
    with pytest.raises(AttributeError):
        models.FileDataSink._resolve_write_func(
            "tests.datasinks.test_writefunc.nonexistent_func"
        )


# --------------------------------------------------------------------------- #
# YAML Roundtrip                                                              #
# --------------------------------------------------------------------------- #


def test_yaml_roundtrip():
    sink = models.FileDataSink(
        path="./my_table/",
        format="DELTA",
        write_func="tests.datasinks.test_writefunc._dummy_write",
    )
    data = sink.model_dump(exclude_none=True)
    sink2 = models.FileDataSink.model_validate(data)
    assert sink2.write_func == sink.write_func


# --------------------------------------------------------------------------- #
# Write - Batch                                                               #
# --------------------------------------------------------------------------- #


def test_batch(tmp_path):
    global _write_path
    target_path = str(tmp_path / "target")
    _write_path = target_path

    sink = models.FileDataSink(
        path=target_path,
        format="DELTA",
        write_func="tests.datasinks.test_writefunc._append_write",
        dataframe_api="NARWHALS",
    )
    sink.write(_build_source_df())

    result = spark.read.format("DELTA").load(target_path).toPandas()
    assert len(result) == 3
    assert set(result["symbol"].tolist()) == {"S0", "S1", "S2"}


# --------------------------------------------------------------------------- #
# Write - Stream                                                              #
# --------------------------------------------------------------------------- #


def test_stream(tmp_path):
    global _write_path
    source_path = str(tmp_path / "source")
    target_path = str(tmp_path / "target")
    checkpoint_path = str(tmp_path / "checkpoint")
    _write_path = target_path

    # Write source as Delta then read back as stream
    _build_source_df().write.format("DELTA").mode("overwrite").save(source_path)
    streaming_df = spark.readStream.format("DELTA").load(source_path)

    sink = models.FileDataSink(
        path=target_path,
        format="DELTA",
        checkpoint_path=checkpoint_path,
        write_func="tests.datasinks.test_writefunc._append_write",
        dataframe_api="NARWHALS",
    )
    sink.write(streaming_df)

    result = spark.read.format("DELTA").load(target_path).toPandas()
    assert len(result) == 3
    assert set(result["symbol"].tolist()) == {"S0", "S1", "S2"}

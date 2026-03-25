import pandas as pd
import pytest

import laktory
from laktory import models

spark = laktory.get_spark_session()


# --------------------------------------------------------------------------- #
# Custom write functions (must be module-level for importlib resolution)      #
# --------------------------------------------------------------------------- #


def _dummy_write(df, node=None) -> None:
    pass


def _append_write(df, target_path=None, node=None) -> None:
    df.to_native().write.format("DELTA").mode("APPEND").save(target_path)


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
        write_func={"func_name": "tests.datasinks.test_writefunc._dummy_write"},
    )
    assert sink.write_func.func_name == "tests.datasinks.test_writefunc._dummy_write"
    assert sink.mode is None


def test_write_func_string_coercion():
    sink = models.FileDataSink(
        path="./my_table/",
        format="DELTA",
        write_func="tests.datasinks.test_writefunc._dummy_write",
    )
    assert isinstance(sink.write_func, models.DataSinkWriter)
    assert sink.write_func.func_name == "tests.datasinks.test_writefunc._dummy_write"
    assert sink.write_func.func_args == []
    assert sink.write_func.func_kwargs == {}


def test_write_func_kwargs():
    sink = models.FileDataSink(
        path="./my_table/",
        format="DELTA",
        write_func={
            "func_name": "tests.datasinks.test_writefunc._append_write",
            "func_kwargs": {"target_path": "/tmp/test"},
        },
    )
    assert sink.write_func.func_kwargs == {"target_path": "/tmp/test"}


def test_write_func_reserved_node_key():
    with pytest.raises(ValueError, match="reserved"):
        models.DataSinkWriter(
            func_name="tests.datasinks.test_writefunc._dummy_write",
            func_kwargs={"node": "something"},
        )


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
    writer = models.DataSinkWriter(
        func_name="tests.datasinks.test_writefunc._dummy_write"
    )
    assert writer._resolve_func() is _dummy_write


def test_resolve_write_func_missing_module():
    writer = models.DataSinkWriter(func_name="nonexistent.module.my_func")
    with pytest.raises(ModuleNotFoundError):
        writer._resolve_func()


def test_resolve_write_func_missing_attr():
    writer = models.DataSinkWriter(
        func_name="tests.datasinks.test_writefunc.nonexistent_func"
    )
    with pytest.raises(AttributeError):
        writer._resolve_func()


# --------------------------------------------------------------------------- #
# YAML Roundtrip                                                              #
# --------------------------------------------------------------------------- #


def test_yaml_roundtrip():
    sink = models.FileDataSink(
        path="./my_table/",
        format="DELTA",
        write_func={
            "func_name": "tests.datasinks.test_writefunc._dummy_write",
            "func_kwargs": {"extra": "value"},
        },
    )
    data = sink.model_dump(exclude_none=True)
    sink2 = models.FileDataSink.model_validate(data)
    assert sink2.write_func.func_name == sink.write_func.func_name
    assert sink2.write_func.func_kwargs == sink.write_func.func_kwargs


# --------------------------------------------------------------------------- #
# Write - Batch                                                               #
# --------------------------------------------------------------------------- #


def test_batch(tmp_path):
    target_path = str(tmp_path / "target")

    sink = models.FileDataSink(
        path=target_path,
        format="DELTA",
        write_func={
            "func_name": "tests.datasinks.test_writefunc._append_write",
            "func_kwargs": {"target_path": target_path},
        },
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
    source_path = str(tmp_path / "source")
    target_path = str(tmp_path / "target")
    checkpoint_path = str(tmp_path / "checkpoint")

    # Write source as Delta then read back as stream
    _build_source_df().write.format("DELTA").mode("overwrite").save(source_path)
    streaming_df = spark.readStream.format("DELTA").load(source_path)

    sink = models.FileDataSink(
        path=target_path,
        format="DELTA",
        checkpoint_path=checkpoint_path,
        write_func={
            "func_name": "tests.datasinks.test_writefunc._append_write",
            "func_kwargs": {"target_path": target_path},
        },
        dataframe_api="NARWHALS",
    )
    sink.write(streaming_df)

    result = spark.read.format("DELTA").load(target_path).toPandas()
    assert len(result) == 3
    assert set(result["symbol"].tolist()) == {"S0", "S1", "S2"}

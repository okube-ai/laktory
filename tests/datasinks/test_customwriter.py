import pandas as pd
import pytest

import laktory
from laktory import models
from laktory.models import LaktoryContext

spark = laktory.get_spark_session()


# --------------------------------------------------------------------------- #
# Custom write functions (must be module-level for importlib resolution)      #
# --------------------------------------------------------------------------- #


def _dummy_write(df) -> None:
    pass


def _append_write(df, target_path=None) -> None:
    df.to_native().write.format("DELTA").mode("APPEND").save(target_path)


def _append_write_native(df, target_path=None) -> None:
    df.write.format("DELTA").mode("APPEND").save(target_path)


def _context_write(
    df, target_path=None, laktory_context: LaktoryContext = None
) -> None:
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


def test_custom_writer_model():
    sink = models.FileDataSink(
        path="./my_table/",
        format="DELTA",
        custom_writer={"func_name": "tests.datasinks.test_customwriter._dummy_write"},
    )
    assert (
        sink.custom_writer.func_name == "tests.datasinks.test_customwriter._dummy_write"
    )
    assert sink.mode is None


def test_custom_writer_string_coercion():
    sink = models.FileDataSink(
        path="./my_table/",
        format="DELTA",
        custom_writer="tests.datasinks.test_customwriter._dummy_write",
    )
    assert isinstance(sink.custom_writer, models.CustomWriter)
    assert (
        sink.custom_writer.func_name == "tests.datasinks.test_customwriter._dummy_write"
    )
    assert sink.custom_writer.func_args == []
    assert sink.custom_writer.func_kwargs == {}


def test_custom_writer_kwargs():
    sink = models.FileDataSink(
        path="./my_table/",
        format="DELTA",
        custom_writer={
            "func_name": "tests.datasinks.test_customwriter._append_write",
            "func_kwargs": {"target_path": "/tmp/test"},
        },
    )
    assert sink.custom_writer.func_kwargs == {"target_path": "/tmp/test"}


def test_custom_writer_and_mode_exclusive():
    with pytest.raises(ValueError, match="mutually exclusive"):
        models.FileDataSink(
            path="./my_table/",
            format="DELTA",
            mode="APPEND",
            custom_writer="tests.datasinks.test_customwriter._dummy_write",
        )


def test_custom_writer_and_merge_cdc_options_exclusive():
    with pytest.raises(ValueError, match="mutually exclusive"):
        models.FileDataSink(
            path="./my_table/",
            format="DELTA",
            custom_writer="tests.datasinks.test_customwriter._dummy_write",
            merge_cdc_options=models.DataSinkMergeCDCOptions(
                primary_keys=["symbol"],
            ),
        )


# --------------------------------------------------------------------------- #
# Function Resolution                                                         #
# --------------------------------------------------------------------------- #


def test_resolve_custom_writer():
    writer = models.CustomWriter(
        func_name="tests.datasinks.test_customwriter._dummy_write"
    )
    assert writer._resolve_func() is _dummy_write


def test_resolve_custom_writer_missing_module():
    writer = models.CustomWriter(func_name="nonexistent.module.my_func")
    with pytest.raises(ModuleNotFoundError):
        writer._resolve_func()


def test_resolve_custom_writer_missing_attr():
    writer = models.CustomWriter(
        func_name="tests.datasinks.test_customwriter.nonexistent_func"
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
        custom_writer={
            "func_name": "tests.datasinks.test_customwriter._dummy_write",
            "func_kwargs": {"extra": "value"},
        },
    )
    data = sink.model_dump(exclude_none=True)
    sink2 = models.FileDataSink.model_validate(data)
    assert sink2.custom_writer.func_name == sink.custom_writer.func_name
    assert sink2.custom_writer.func_kwargs == sink.custom_writer.func_kwargs


# --------------------------------------------------------------------------- #
# LaktoryContext injection                                                    #
# --------------------------------------------------------------------------- #

# Captured context for assertion across the module boundary
_captured_context: LaktoryContext = None


def _capture_context_write(df, laktory_context: LaktoryContext = None) -> None:
    global _captured_context
    _captured_context = laktory_context

    df.to_native().write.format("DELTA").mode("APPEND").save(laktory_context.sink.path)


def test_laktory_context(tmp_path):
    global _captured_context
    target_path = str(tmp_path / "target")

    sink = models.FileDataSink(
        path=target_path,
        format="DELTA",
        custom_writer={
            "func_name": "tests.datasinks.test_customwriter._capture_context_write",
        },
        dataframe_api="NARWHALS",
    )
    sink.write(_build_source_df())

    assert isinstance(_captured_context, LaktoryContext)
    assert _captured_context.sink is sink
    assert _captured_context.node is None  # standalone sink, no pipeline node
    assert _captured_context.pipeline is None


def test_laktory_context_with_pipeline_node(tmp_path):
    global _captured_context
    target_path = str(tmp_path / "target")

    node = models.PipelineNode(
        name="test_node",
        sinks=[
            models.FileDataSink(
                path=target_path,
                format="DELTA",
                custom_writer={
                    "func_name": "tests.datasinks.test_customwriter._capture_context_write",
                },
                dataframe_api="NARWHALS",
            )
        ],
    )
    sink = node.sinks[0]
    sink.write(_build_source_df())

    assert isinstance(_captured_context, LaktoryContext)
    assert _captured_context.sink is sink
    assert _captured_context.node is node
    assert _captured_context.pipeline is None


def test_laktory_context_not_injected_when_not_declared(tmp_path):
    """Functions without laktory_context in their signature receive no injection."""
    target_path = str(tmp_path / "target")

    sink = models.FileDataSink(
        path=target_path,
        format="DELTA",
        custom_writer={
            "func_name": "tests.datasinks.test_customwriter._append_write",
            "func_kwargs": {"target_path": target_path},
        },
        dataframe_api="NARWHALS",
    )
    # Would raise TypeError if laktory_context were injected into _append_write
    sink.write(_build_source_df())

    result = spark.read.format("DELTA").load(target_path).toPandas()
    assert len(result) == 3


# --------------------------------------------------------------------------- #
# Write - Batch                                                               #
# --------------------------------------------------------------------------- #


def test_batch(tmp_path):
    target_path = str(tmp_path / "target")

    sink = models.FileDataSink(
        path=target_path,
        format="DELTA",
        custom_writer={
            "func_name": "tests.datasinks.test_customwriter._append_write",
            "func_kwargs": {"target_path": target_path},
        },
        dataframe_api="NARWHALS",
    )
    sink.write(_build_source_df())

    result = spark.read.format("DELTA").load(target_path).toPandas()
    assert len(result) == 3
    assert set(result["symbol"].tolist()) == {"S0", "S1", "S2"}


def test_batch_native(tmp_path):
    target_path = str(tmp_path / "target")

    sink = models.FileDataSink(
        path=target_path,
        format="DELTA",
        custom_writer={
            "func_name": "tests.datasinks.test_customwriter._append_write_native",
            "func_kwargs": {"target_path": target_path},
        },
        dataframe_api="NATIVE",
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
        custom_writer={
            "func_name": "tests.datasinks.test_customwriter._append_write",
            "func_kwargs": {"target_path": target_path},
        },
        dataframe_api="NARWHALS",
    )
    sink.write(streaming_df)

    result = spark.read.format("DELTA").load(target_path).toPandas()
    assert len(result) == 3
    assert set(result["symbol"].tolist()) == {"S0", "S1", "S2"}


def test_laktory_context_stream_with_pipeline_node(tmp_path):
    global _captured_context
    source_path = str(tmp_path / "source")
    target_path = str(tmp_path / "target")
    checkpoint_path = str(tmp_path / "checkpoint")

    _build_source_df().write.format("DELTA").mode("overwrite").save(source_path)
    streaming_df = spark.readStream.format("DELTA").load(source_path)

    node = models.PipelineNode(
        name="test_node",
        sinks=[
            models.FileDataSink(
                path=target_path,
                format="DELTA",
                checkpoint_path=checkpoint_path,
                custom_writer={
                    "func_name": "tests.datasinks.test_customwriter._capture_context_write",
                },
                dataframe_api="NARWHALS",
            )
        ],
    )
    sink = node.sinks[0]
    sink.write(streaming_df)

    assert isinstance(_captured_context, LaktoryContext)
    assert _captured_context.sink is sink
    assert _captured_context.node is node
    assert _captured_context.pipeline is None

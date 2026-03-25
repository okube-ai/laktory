import pytest

from laktory import models
from laktory.models.datasinks.foreachbatchoptions import DataSinkForEachBatchOptions

# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #


def _dummy_batch_func(batch_df, batch_id: int) -> None:
    pass


# --------------------------------------------------------------------------- #
# Model Validation                                                            #
# --------------------------------------------------------------------------- #


def test_foreach_batch_options_model():
    opts = DataSinkForEachBatchOptions(
        func="tests.datasinks.test_foreach_batch._dummy_batch_func"
    )
    assert opts.func == "tests.datasinks.test_foreach_batch._dummy_batch_func"


def test_foreach_batch_mode_requires_options():
    with pytest.raises(ValueError, match="foreach_batch_options"):
        models.FileDataSink(
            path="./my_table/",
            format="DELTA",
            mode="FOREACH_BATCH",
        )


def test_foreach_batch_mode_with_options():
    sink = models.FileDataSink(
        path="./my_table/",
        format="DELTA",
        mode="FOREACH_BATCH",
        foreach_batch_options={
            "func": "tests.datasinks.test_foreach_batch._dummy_batch_func"
        },
    )
    assert sink.mode == "FOREACH_BATCH"
    assert sink.foreach_batch_options is not None
    # Back-reference to parent sink is set
    assert sink.foreach_batch_options.sink is sink


# --------------------------------------------------------------------------- #
# Function Resolution                                                         #
# --------------------------------------------------------------------------- #


def test_resolve_func():
    opts = DataSinkForEachBatchOptions(
        func="tests.datasinks.test_foreach_batch._dummy_batch_func"
    )
    resolved = opts._resolve_func()
    assert resolved is _dummy_batch_func


def test_resolve_func_missing_module():
    opts = DataSinkForEachBatchOptions(func="nonexistent.module.my_func")
    with pytest.raises(ModuleNotFoundError):
        opts._resolve_func()


def test_resolve_func_missing_attr():
    opts = DataSinkForEachBatchOptions(
        func="tests.datasinks.test_foreach_batch.nonexistent_func"
    )
    with pytest.raises(AttributeError):
        opts._resolve_func()


# --------------------------------------------------------------------------- #
# YAML Roundtrip                                                              #
# --------------------------------------------------------------------------- #


def test_yaml_roundtrip():
    sink = models.FileDataSink(
        path="./my_table/",
        format="DELTA",
        mode="FOREACH_BATCH",
        foreach_batch_options={
            "func": "tests.datasinks.test_foreach_batch._dummy_batch_func"
        },
    )
    data = sink.model_dump(exclude_none=True)
    sink2 = models.FileDataSink.model_validate(data)
    assert sink2.mode == "FOREACH_BATCH"
    assert sink2.foreach_batch_options.func == sink.foreach_batch_options.func

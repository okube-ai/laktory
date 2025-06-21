from laktory.models import DLTViewDataSink


def test_view():
    sink = DLTViewDataSink(
        dlt_view_name="t",
    )
    assert sink.dlt_name == "t"

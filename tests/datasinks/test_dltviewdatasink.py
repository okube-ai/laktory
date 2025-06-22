from laktory.models import PipelineViewDataSink


def test_view():
    sink = PipelineViewDataSink(
        pipeline_view_name="t",
    )
    assert sink.dlt_name == "t"

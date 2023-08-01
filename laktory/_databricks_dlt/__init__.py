from .api import *
create_view = view
create_table = table
create_streaming_table = create_streaming_live_table
readStream = read_stream
__all__ = [
    "create_view",
    "create_table",
    "create_streaming_live_table",
    "create_streaming_table",
    "create_target_table",
    "view",
    "table",
    "append_flow",
    "read", "read_stream",
    "expect",
    "expect_or_fail",
    "expect_or_drop",
    "expect_all_or_fail",
    "expect_all_or_drop",
    "expect_all",
    "apply_changes",
    "apply_changes_from_snapshot",
    "create_feature_table"
]

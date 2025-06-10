import os

from .dataframecompare import assert_dfs_equal
from .get_dfs import StreamingSource
from .get_dfs import get_df0
from .get_dfs import get_df1
from .paths import Paths


def skip_test(required, extras=None):
    import pytest

    if extras is not None:
        for e in extras:
            required += [e]

    missing = []
    for k in required:
        if not os.getenv(k):
            missing += [k]
    if missing:
        pytest.skip(f"{missing} not available.")


def skip_pulumi_preview(extras=None):
    skip_test(
        required=[
            "PULUMI_ACCESS_TOKEN",
            "DATABRICKS_HOST",
            "DATABRICKS_TOKEN",
        ],
        extras=extras,
    )


def skip_terraform_plan(extras=None):
    skip_test(
        required=[
            "DATABRICKS_HOST",
            "DATABRICKS_TOKEN",
        ],
        extras=extras,
    )

"""
Tests for AirflowOrchestrator.

Covers:
  - DAG attributes (schedule, start_date, macros, timeouts, tags)
  - End-to-end Airflow execution (skipped when Airflow is not initialized)
"""

from datetime import datetime

import polars
import pytest

from laktory import models


def _get_pl(tmp_path=""):
    return models.Pipeline.model_validate(
        {
            "name": "pl-simple",
            "orchestrator": {
                "type": "AIRFLOW",
                "schedule": {"cron": "0 0 * * *", "timezone": "utc"},
                "start_date": "2026-03-03",
                "user_defined_macros": {"a": 1},
                "user_defined_filters": {"b": 2},
                "default_args": {"full": True},
                "max_active_tasks": 2,
                "max_active_runs": 1,
                "dagrun_timeout": "60s",
                "catchup": False,
                "access_control": {"a": ["1", "2"]},
                "tags": ["pipeline", "stocks"],
                "fail_fast": False,
            },
            "dataframe_backend": "POLARS",
            "nodes": [
                {
                    "name": "brz",
                    "source": {
                        "data": {
                            "_idx": [0, 1, 2],
                            "id": ["a", "b", "c"],
                            "x1": [1, 2, 3],
                        }
                    },
                    "sinks": [{"format": "PARQUET", "path": f"{tmp_path}/brz.parquet"}],
                },
                {
                    "name": "slv",
                    "source": {"node_name": "brz"},
                    "transformer": {
                        "nodes": [
                            {"func_name": "with_columns", "func_kwargs": {"y1": "x1"}},
                            {"expr": "SELECT id, x1, y1 from {df}"},
                        ]
                    },
                },
                {
                    "name": "gld",
                    "transformer": {
                        "nodes": [
                            {
                                "expr": "SELECT id, MAX(x1) AS max_x1 from {nodes.slv} GROUP BY id"
                            }
                        ]
                    },
                    "sinks": [{"format": "PARQUET", "path": f"{tmp_path}/gld.parquet"}],
                },
            ],
        }
    )


def is_airflow_initialized():
    try:
        from airflow.configuration import conf
        from sqlalchemy import create_engine
        from sqlalchemy import text

        conn = conf.get("database", "sql_alchemy_conn")
        engine = create_engine(conn)
        with engine.connect() as c:
            c.execute(text("SELECT 1 FROM task_instance LIMIT 1"))
        return True
    except Exception:
        return False


# --------------------------------------------------------------------------- #
# DAG attributes                                                              #
# --------------------------------------------------------------------------- #


def test_dag_attributes():
    from datetime import timezone

    import jinja2
    from airflow.timetables.interval import CronDataIntervalTimetable
    from pendulum.tz.timezone import UTC

    pl = _get_pl()
    dag = pl.to_airflow_dag(template_undefined=jinja2.DebugUndefined)

    assert len(dag.tasks) == 2  # brz + gld; slv has no sink so no task
    assert dag.schedule == CronDataIntervalTimetable(cron="0 0 * * *", timezone=UTC)
    assert dag.start_date == datetime(2026, 3, 3, 0, 0, 0, tzinfo=timezone.utc)
    assert dag.user_defined_macros == {"a": 1}
    assert dag.max_active_tasks == 2
    from datetime import timedelta

    assert dag.dagrun_timeout == timedelta(seconds=60)
    assert not dag.catchup
    assert dag.access_control == {"a": {"DAGs": {"1", "2"}}}
    assert dag.tags == {"stocks", "pipeline"}
    assert not dag.fail_fast


# --------------------------------------------------------------------------- #
# End-to-end execution                                                        #
# --------------------------------------------------------------------------- #


@pytest.mark.skipif(
    not is_airflow_initialized(),
    reason="Airflow integration test disabled",
)
def test_execute(tmp_path):
    pl = _get_pl(tmp_path)
    dag = pl.to_airflow_dag()
    dag.test()

    df_brz = polars.read_parquet(tmp_path / "brz.parquet").sort("id")
    df_gld = polars.read_parquet(tmp_path / "gld.parquet").sort("id")

    assert df_brz.to_dicts() == [
        {"_idx": 0, "id": "a", "x1": 1},
        {"_idx": 1, "id": "b", "x1": 2},
        {"_idx": 2, "id": "c", "x1": 3},
    ]
    assert df_gld.to_dicts() == [
        {"id": "a", "max_x1": 1},
        {"id": "b", "max_x1": 2},
        {"id": "c", "max_x1": 3},
    ]

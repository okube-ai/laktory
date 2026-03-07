import io
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import polars
from pendulum.tz.timezone import UTC

from laktory import models

data_dirpath = Path(__file__).parent.parent.parent / "data"

OPEN_FIGURES = False


def get_pl(tmp_path):
    with open(data_dirpath / "pl_simple.yaml") as fp:
        data = fp.read()
        data = data.replace("{tmp_path}", str(tmp_path))
        pl = models.Pipeline.model_validate_yaml(io.StringIO(data))
        pl.root_path_ = tmp_path

    return pl


def test_execute(tmp_path):
    from datetime import timezone

    import jinja2
    from airflow.timetables.interval import CronDataIntervalTimetable

    pl = get_pl(tmp_path)

    # Get Airflow dag
    dag = pl.to_airflow_dag(
        template_undefined=jinja2.DebugUndefined,
    )
    assert len(dag.tasks) == 2
    assert dag.schedule == CronDataIntervalTimetable(
        cron="0 0 * * *",
        timezone=UTC,
    )
    assert dag.start_date == datetime(2026, 3, 3, 0, 0, 0, tzinfo=timezone.utc)
    assert dag.user_defined_macros == {"a": 1}
    assert dag.max_active_tasks == 2
    assert dag.dagrun_timeout == timedelta(seconds=60)
    assert not dag.catchup
    assert dag.access_control == {"a": {"DAGs": {"1", "2"}}}
    assert dag.tags == {"stocks", "pipeline"}
    assert dag.auto_register
    assert not dag.fail_fast
    assert dag.template_undefined == jinja2.DebugUndefined

    # Execute
    dag.test()
    df_brz = polars.read_parquet(tmp_path / "brz.parquet").sort("id")
    # df_slv = polars.read_parquet(tmp_path/"slv.parquet").sort("id")
    df_gld = polars.read_parquet(tmp_path / "gld.parquet").sort("id")
    assert df_brz.to_dicts() == [
        {"_idx": 0, "id": "a", "x1": 1},
        {"_idx": 1, "id": "b", "x1": 2},
        {"_idx": 2, "id": "c", "x1": 3},
    ]
    # assert df_slv.to_dicts() == [{'id': 'a', 'x1': 1, 'y1': 1}, {'id': 'b', 'x1': 2, 'y1': 2}, {'id': 'c', 'x1': 3, 'y1': 3}]
    assert df_gld.to_dicts() == [
        {"id": "a", "max_x1": 1},
        {"id": "b", "max_x1": 2},
        {"id": "c", "max_x1": 3},
    ]

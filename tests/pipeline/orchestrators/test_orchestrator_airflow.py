import io
from pathlib import Path

from laktory import models

# from ./../conftest import assert_dfs_equal

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
    backend = "POLARS"

    pl = get_pl(tmp_path)
    pl.dataframe_backend_ = backend

    # Set Orchestrator
    pl.orchestrator = models.AirflowOrchestrator()

    # Execute
    dag = pl.to_airflow_dag()

    # Test
    print(dag)
    dag.test()
    assert 1 == 2

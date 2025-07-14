import io
import json
import sys
from pathlib import Path

import laktory as lk
from laktory import __version__
from laktory import models
from laktory.enums import DataFrameBackends

data_dirpath = Path(__file__).parent.parent / "data"
testdir_path = Path(__file__).parent


def get_pl(tmp_path="", is_dlt=False):
    filepath = data_dirpath / "pl.yaml"
    if is_dlt:
        filepath = data_dirpath / "pl_dlt.yaml"

    with open(filepath) as fp:
        data = fp.read()
        data = data.replace("{tmp_path}", str(tmp_path))
        pl = models.Pipeline.model_validate_yaml(io.StringIO(data))

        pl.root_path_ = tmp_path

    return pl


# Job
def get_pl_job():
    pl = get_pl()
    pl.name = "pl-job"
    # pl.dependencies = ["yfinance"]
    pl.orchestrator = {
        "job_clusters": [
            {
                "job_cluster_key": "node-cluster",
                "new_cluster": {
                    "node_type_id": "Standard_DS3_v2",
                    "spark_version": "16.3.x-scala2.12",
                },
            }
        ],
        "name": "pl-job",
        "type": "DATABRICKS_JOB",
    }

    return pl


# DLT
def get_pl_dlt():
    pl = get_pl(is_dlt=True)
    return pl


def test_databricks_job():
    # Test job
    job = get_pl_job().orchestrator
    data = job.model_dump(exclude_unset=True)
    data = json.loads(
        json.dumps(data).replace(f"laktory=={__version__}", "laktory==__version__")
    )
    print(data)
    assert data == {
        "environments": [
            {
                "environment_key": "laktory",
                "spec": {
                    "client": "3",
                    "dependencies": [
                        "requests>=2.0",
                        "./wheels/lake-0.0.1-py3-none-any.whl",
                        "laktory==__version__",
                    ],
                },
            }
        ],
        "job_clusters": [
            {
                "job_cluster_key": "node-cluster",
                "new_cluster": {
                    "node_type_id": "Standard_DS3_v2",
                    "spark_version": "16.3.x-scala2.12",
                },
            }
        ],
        "name": "pl-job",
        "parameters": [{"default": "false", "name": "full_refresh"}],
        "tasks": [
            {
                "depends_ons": [],
                "job_cluster_key": "node-cluster",
                "libraries": [
                    {"pypi": {"package": "requests>=2.0"}},
                    {"whl": "./wheels/lake-0.0.1-py3-none-any.whl"},
                    {"pypi": {"package": "laktory==__version__"}},
                ],
                "python_wheel_task": {
                    "entry_point": "models.pipeline._read_and_execute",
                    "named_parameters": {
                        "filepath": "/Workspace/.laktory/pipelines/pl-job/config.json",
                        "node_name": "brz",
                    },
                    "package_name": "laktory",
                },
                "task_key": "node-brz",
            },
            {
                "depends_ons": [{"task_key": "node-slv"}],
                "job_cluster_key": "node-cluster",
                "libraries": [
                    {"pypi": {"package": "requests>=2.0"}},
                    {"whl": "./wheels/lake-0.0.1-py3-none-any.whl"},
                    {"pypi": {"package": "laktory==__version__"}},
                ],
                "python_wheel_task": {
                    "entry_point": "models.pipeline._read_and_execute",
                    "named_parameters": {
                        "filepath": "/Workspace/.laktory/pipelines/pl-job/config.json",
                        "node_name": "gld",
                    },
                    "package_name": "laktory",
                },
                "task_key": "node-gld",
            },
            {
                "depends_ons": [{"task_key": "node-gld"}],
                "job_cluster_key": "node-cluster",
                "libraries": [
                    {"pypi": {"package": "requests>=2.0"}},
                    {"whl": "./wheels/lake-0.0.1-py3-none-any.whl"},
                    {"pypi": {"package": "laktory==__version__"}},
                ],
                "python_wheel_task": {
                    "entry_point": "models.pipeline._read_and_execute",
                    "named_parameters": {
                        "filepath": "/Workspace/.laktory/pipelines/pl-job/config.json",
                        "node_name": "gld_a",
                    },
                    "package_name": "laktory",
                },
                "task_key": "node-gld_a",
            },
            {
                "depends_ons": [{"task_key": "node-gld_a"}, {"task_key": "node-gld_b"}],
                "job_cluster_key": "node-cluster",
                "libraries": [
                    {"pypi": {"package": "requests>=2.0"}},
                    {"whl": "./wheels/lake-0.0.1-py3-none-any.whl"},
                    {"pypi": {"package": "laktory==__version__"}},
                ],
                "python_wheel_task": {
                    "entry_point": "models.pipeline._read_and_execute",
                    "named_parameters": {
                        "filepath": "/Workspace/.laktory/pipelines/pl-job/config.json",
                        "node_name": "gld_ab",
                    },
                    "package_name": "laktory",
                },
                "task_key": "node-gld_ab",
            },
            {
                "depends_ons": [{"task_key": "node-gld"}],
                "job_cluster_key": "node-cluster",
                "libraries": [
                    {"pypi": {"package": "requests>=2.0"}},
                    {"whl": "./wheels/lake-0.0.1-py3-none-any.whl"},
                    {"pypi": {"package": "laktory==__version__"}},
                ],
                "python_wheel_task": {
                    "entry_point": "models.pipeline._read_and_execute",
                    "named_parameters": {
                        "filepath": "/Workspace/.laktory/pipelines/pl-job/config.json",
                        "node_name": "gld_b",
                    },
                    "package_name": "laktory",
                },
                "task_key": "node-gld_b",
            },
            {
                "depends_ons": [{"task_key": "node-brz"}],
                "job_cluster_key": "node-cluster",
                "libraries": [
                    {"pypi": {"package": "requests>=2.0"}},
                    {"whl": "./wheels/lake-0.0.1-py3-none-any.whl"},
                    {"pypi": {"package": "laktory==__version__"}},
                ],
                "python_wheel_task": {
                    "entry_point": "models.pipeline._read_and_execute",
                    "named_parameters": {
                        "filepath": "/Workspace/.laktory/pipelines/pl-job/config.json",
                        "node_name": "slv",
                    },
                    "package_name": "laktory",
                },
                "task_key": "node-slv",
            },
        ],
        "type": "DATABRICKS_JOB",
        "dataframe_backend": "PYSPARK",
        "dataframe_api": "NARWHALS",
    }

    # Test resources
    resources = job.core_resources
    assert len(resources) == 3

    config = job.config_file
    print(config.content_dict)
    assert config.content_dict == {
        "dependencies": ["requests>=2.0", "./wheels/lake-0.0.1-py3-none-any.whl"],
        "imports": ["re"],
        "name": "pl-job",
        "nodes": [
            {
                "name": "gld_ab",
                "sinks": [
                    {
                        "schema_name": "default",
                        "table_name": "gld_ab",
                        "table_type": "VIEW",
                        "view_definition": {
                            "expr": "SELECT * from {nodes.gld_a} UNION SELECT * from {nodes.gld_b}",
                            "dataframe_backend": "PYSPARK",
                            "dataframe_api": "NARWHALS",
                        },
                        "dataframe_backend": "PYSPARK",
                        "dataframe_api": "NARWHALS",
                        "checkpoint_path": "pipelines/pl-job/gld_ab/checkpoints/sink-cd086c7d-37a3-0a0b-490b-96fa2980679e",
                    }
                ],
                "dataframe_backend": "PYSPARK",
                "dataframe_api": "NARWHALS",
                "root_path": "pipelines/pl-job/gld_ab",
                "expectations_checkpoint_path": "pipelines/pl-job/gld_ab/checkpoints/expectations",
            },
            {
                "name": "brz",
                "source": {
                    "format": "JSON",
                    "path": "/brz_source/",
                    "dataframe_backend": "PYSPARK",
                    "dataframe_api": "NARWHALS",
                    "schema_location": "/",
                },
                "sinks": [
                    {
                        "mode": "APPEND",
                        "format": "PARQUET",
                        "path": "/brz_sink/",
                        "dataframe_backend": "PYSPARK",
                        "dataframe_api": "NARWHALS",
                        "checkpoint_path": "pipelines/pl-job/brz/checkpoints/sink-a85fc0d1-a207-3224-b4af-406390f4510d",
                    }
                ],
                "dataframe_backend": "PYSPARK",
                "dataframe_api": "NARWHALS",
                "root_path": "pipelines/pl-job/brz",
                "expectations_checkpoint_path": "pipelines/pl-job/brz/checkpoints/expectations",
            },
            {
                "name": "slv",
                "source": {
                    "node_name": "brz",
                    "dataframe_backend": "PYSPARK",
                    "dataframe_api": "NARWHALS",
                },
                "sinks": [
                    {
                        "mode": "APPEND",
                        "format": "DELTA",
                        "path": "/slv_sink/",
                        "dataframe_backend": "PYSPARK",
                        "dataframe_api": "NARWHALS",
                        "checkpoint_path": "pipelines/pl-job/slv/checkpoints/sink-69b8e949-8f1b-b0b6-daee-57262072499d",
                    }
                ],
                "transformer": {
                    "nodes": [
                        {
                            "func_kwargs": {
                                "y1": {
                                    "value": "x1",
                                    "dataframe_backend": "PYSPARK",
                                    "dataframe_api": "NARWHALS",
                                }
                            },
                            "func_name": "with_columns",
                            "dataframe_backend": "PYSPARK",
                            "dataframe_api": "NARWHALS",
                        },
                        {
                            "expr": "SELECT id, x1, y1 from {df}",
                            "dataframe_backend": "PYSPARK",
                            "dataframe_api": "NARWHALS",
                        },
                    ],
                    "dataframe_backend": "PYSPARK",
                    "dataframe_api": "NARWHALS",
                },
                "dataframe_backend": "PYSPARK",
                "dataframe_api": "NARWHALS",
                "root_path": "pipelines/pl-job/slv",
                "expectations_checkpoint_path": "pipelines/pl-job/slv/checkpoints/expectations",
            },
            {
                "name": "gld",
                "sinks": [
                    {
                        "mode": "OVERWRITE",
                        "writer_kwargs": {"path": "/gld_sink/"},
                        "format": "PARQUET",
                        "schema_name": "default",
                        "table_name": "gld",
                        "dataframe_backend": "PYSPARK",
                        "dataframe_api": "NARWHALS",
                        "checkpoint_path": "pipelines/pl-job/gld/checkpoints/sink-e25d455a-7800-fd95-78a7-12db180593f8",
                    }
                ],
                "transformer": {
                    "nodes": [
                        {
                            "expr": "SELECT id, MAX(x1) AS max_x1 from {nodes.slv} GROUP BY id",
                            "dataframe_backend": "PYSPARK",
                            "dataframe_api": "NARWHALS",
                        }
                    ],
                    "dataframe_backend": "PYSPARK",
                    "dataframe_api": "NARWHALS",
                },
                "dataframe_backend": "PYSPARK",
                "dataframe_api": "NARWHALS",
                "root_path": "pipelines/pl-job/gld",
                "expectations_checkpoint_path": "pipelines/pl-job/gld/checkpoints/expectations",
            },
            {
                "name": "gld_a",
                "sinks": [
                    {
                        "schema_name": "default",
                        "table_name": "gld_a",
                        "table_type": "VIEW",
                        "view_definition": {
                            "expr": "SELECT * from {nodes.gld} WHERE id = 'a'",
                            "dataframe_backend": "PYSPARK",
                            "dataframe_api": "NARWHALS",
                        },
                        "dataframe_backend": "PYSPARK",
                        "dataframe_api": "NARWHALS",
                        "checkpoint_path": "pipelines/pl-job/gld_a/checkpoints/sink-514b35b8-117e-9aa5-362b-5dc5b3ece569",
                    }
                ],
                "dataframe_backend": "PYSPARK",
                "dataframe_api": "NARWHALS",
                "root_path": "pipelines/pl-job/gld_a",
                "expectations_checkpoint_path": "pipelines/pl-job/gld_a/checkpoints/expectations",
            },
            {
                "name": "gld_b",
                "sinks": [
                    {
                        "schema_name": "default",
                        "table_name": "gld_b",
                        "table_type": "VIEW",
                        "view_definition": {
                            "expr": "SELECT * from {nodes.gld} WHERE id = 'b'",
                            "dataframe_backend": "PYSPARK",
                            "dataframe_api": "NARWHALS",
                        },
                        "dataframe_backend": "PYSPARK",
                        "dataframe_api": "NARWHALS",
                        "checkpoint_path": "pipelines/pl-job/gld_b/checkpoints/sink-d897b6e7-771c-6620-a110-d2fd1c8c9ae3",
                    }
                ],
                "dataframe_backend": "PYSPARK",
                "dataframe_api": "NARWHALS",
                "root_path": "pipelines/pl-job/gld_b",
                "expectations_checkpoint_path": "pipelines/pl-job/gld_b/checkpoints/expectations",
            },
        ],
        "dataframe_backend": "PYSPARK",
        "dataframe_api": "NARWHALS",
        "root_path": "pipelines/pl-job",
        "orchestrator": {
            "environments": [
                {
                    "environment_key": "laktory",
                    "spec": {
                        "client": "3",
                        "dependencies": [
                            "requests>=2.0",
                            "./wheels/lake-0.0.1-py3-none-any.whl",
                            "laktory==0.8.3",
                        ],
                    },
                }
            ],
            "job_clusters": [
                {
                    "job_cluster_key": "node-cluster",
                    "new_cluster": {
                        "node_type_id": "Standard_DS3_v2",
                        "spark_version": "16.3.x-scala2.12",
                    },
                }
            ],
            "name": "pl-job",
            "parameters": [{"default": "false", "name": "full_refresh"}],
            "tasks": [
                {
                    "depends_ons": [],
                    "job_cluster_key": "node-cluster",
                    "libraries": [
                        {"pypi": {"package": "requests>=2.0"}},
                        {"whl": "./wheels/lake-0.0.1-py3-none-any.whl"},
                        {"pypi": {"package": "laktory==0.8.3"}},
                    ],
                    "python_wheel_task": {
                        "entry_point": "models.pipeline._read_and_execute",
                        "named_parameters": {
                            "filepath": "/Workspace/.laktory/pipelines/pl-job/config.json",
                            "node_name": "brz",
                        },
                        "package_name": "laktory",
                    },
                    "task_key": "node-brz",
                },
                {
                    "depends_ons": [{"task_key": "node-slv"}],
                    "job_cluster_key": "node-cluster",
                    "libraries": [
                        {"pypi": {"package": "requests>=2.0"}},
                        {"whl": "./wheels/lake-0.0.1-py3-none-any.whl"},
                        {"pypi": {"package": "laktory==0.8.3"}},
                    ],
                    "python_wheel_task": {
                        "entry_point": "models.pipeline._read_and_execute",
                        "named_parameters": {
                            "filepath": "/Workspace/.laktory/pipelines/pl-job/config.json",
                            "node_name": "gld",
                        },
                        "package_name": "laktory",
                    },
                    "task_key": "node-gld",
                },
                {
                    "depends_ons": [{"task_key": "node-gld"}],
                    "job_cluster_key": "node-cluster",
                    "libraries": [
                        {"pypi": {"package": "requests>=2.0"}},
                        {"whl": "./wheels/lake-0.0.1-py3-none-any.whl"},
                        {"pypi": {"package": "laktory==0.8.3"}},
                    ],
                    "python_wheel_task": {
                        "entry_point": "models.pipeline._read_and_execute",
                        "named_parameters": {
                            "filepath": "/Workspace/.laktory/pipelines/pl-job/config.json",
                            "node_name": "gld_a",
                        },
                        "package_name": "laktory",
                    },
                    "task_key": "node-gld_a",
                },
                {
                    "depends_ons": [
                        {"task_key": "node-gld_a"},
                        {"task_key": "node-gld_b"},
                    ],
                    "job_cluster_key": "node-cluster",
                    "libraries": [
                        {"pypi": {"package": "requests>=2.0"}},
                        {"whl": "./wheels/lake-0.0.1-py3-none-any.whl"},
                        {"pypi": {"package": "laktory==0.8.3"}},
                    ],
                    "python_wheel_task": {
                        "entry_point": "models.pipeline._read_and_execute",
                        "named_parameters": {
                            "filepath": "/Workspace/.laktory/pipelines/pl-job/config.json",
                            "node_name": "gld_ab",
                        },
                        "package_name": "laktory",
                    },
                    "task_key": "node-gld_ab",
                },
                {
                    "depends_ons": [{"task_key": "node-gld"}],
                    "job_cluster_key": "node-cluster",
                    "libraries": [
                        {"pypi": {"package": "requests>=2.0"}},
                        {"whl": "./wheels/lake-0.0.1-py3-none-any.whl"},
                        {"pypi": {"package": "laktory==0.8.3"}},
                    ],
                    "python_wheel_task": {
                        "entry_point": "models.pipeline._read_and_execute",
                        "named_parameters": {
                            "filepath": "/Workspace/.laktory/pipelines/pl-job/config.json",
                            "node_name": "gld_b",
                        },
                        "package_name": "laktory",
                    },
                    "task_key": "node-gld_b",
                },
                {
                    "depends_ons": [{"task_key": "node-brz"}],
                    "job_cluster_key": "node-cluster",
                    "libraries": [
                        {"pypi": {"package": "requests>=2.0"}},
                        {"whl": "./wheels/lake-0.0.1-py3-none-any.whl"},
                        {"pypi": {"package": "laktory==0.8.3"}},
                    ],
                    "python_wheel_task": {
                        "entry_point": "models.pipeline._read_and_execute",
                        "named_parameters": {
                            "filepath": "/Workspace/.laktory/pipelines/pl-job/config.json",
                            "node_name": "slv",
                        },
                        "package_name": "laktory",
                    },
                    "task_key": "node-slv",
                },
            ],
            "type": "DATABRICKS_JOB",
            "dataframe_backend": "PYSPARK",
            "dataframe_api": "NARWHALS",
        },
    }


def test_databricks_job_execute(mocker):
    pl = get_pl_job()

    # Patch the model loader to set tmp_path value and skip execution
    mocker.patch("laktory.models.Pipeline.model_validate_yaml", return_value=pl)
    mocker.patch("laktory.models.PipelineNode.execute", return_value=None)

    # Setup arguments
    test_args = [
        "_read_and_execute",
        "--filepath",
        str(data_dirpath / "pl.yaml"),
        "--node_name",
        "brz",
        "--full_refresh",
        "true",
    ]
    mocker.patch.object(sys, "argv", test_args)

    # Run function
    lk.models.pipeline._read_and_execute()


def test_databricks_pipeline(tmp_path):
    # Mock laktory version to account for dynamically changing value
    lk.__version__ = "<version>"

    pl = get_pl_dlt()

    # Test node names
    assert pl.nodes_dict["brz"].primary_sink.dlt_name == "dev.sandbox.brz"
    assert pl.nodes_dict["slv"].primary_sink.dlt_name == "dev.sandbox.slv"
    assert pl.nodes_dict["gld"].primary_sink.dlt_name == "gld"
    assert pl.nodes_dict["gld_a"].primary_sink.dlt_name == "dev.sandbox2.gld_a"
    assert pl.nodes_dict["gld_b"].primary_sink.dlt_name == "dev.sandbox2.gld_b"
    assert pl.nodes_dict["gld_ab"].primary_sink.dlt_name == "prd.sandbox2.gld_ab"

    # Test Sink as Source
    node_slv = pl.nodes_dict["slv"]
    sink_source = node_slv.source.node.primary_sink.as_source(
        as_stream=node_slv.source.as_stream
    )
    data = sink_source.model_dump()
    assert data.pop("dataframe_backend") == DataFrameBackends.PYSPARK
    print(data)
    assert data == {
        "as_stream": False,
        "drop_duplicates": None,
        "drops": None,
        "filter": None,
        "renames": None,
        "selects": None,
        "type": "UNITY_CATALOG",
        "catalog_name": "dev",
        "schema_name": "sandbox",
        "table_name": "brz",
        "reader_methods": [],
        "dataframe_api": "NARWHALS",
    }

    data = pl.orchestrator.model_dump(mode="json")
    # data["configuration"]["requirements"] = data["configuration"]["requirements"].replace(__version__, "<version>")
    print(data)
    assert data == {
        "access_controls": [
            {
                "group_name": "account users",
                "permission_level": "CAN_VIEW",
                "service_principal_name": None,
                "user_name": None,
            }
        ],
        "allow_duplicate_names": None,
        "budget_policy_id": None,
        "catalog": "dev",
        "cause": None,
        "channel": "PREVIEW",
        "cluster_id": None,
        "clusters": [],
        "creator_user_name": None,
        "configuration": {
            "pipeline_name": "pl-dlt",
            "requirements": '["laktory==<version>"]',
            "config_filepath": "/Workspace/.laktory/pipelines/pl-dlt/config.json",
        },
        "continuous": None,
        "deployment": None,
        "development": None,
        "edition": None,
        "event_log": None,
        "expected_last_modified": None,
        "filters": None,
        "gateway_definition": None,
        "health": None,
        "last_modified": None,
        "latest_updates": None,
        "libraries": None,
        "name": "pl-dlt",
        "name_prefix": None,
        "name_suffix": None,
        "notifications": [],
        "photon": None,
        "restart_window": None,
        "root_path": None,
        "run_as": None,
        "run_as_user_name": None,
        "schema_": "sandbox",
        "serverless": None,
        "state": None,
        "storage": None,
        "target": None,
        "trigger": None,
        "url": None,
        "type": "DATABRICKS_PIPELINE",
        "config_file": {
            "access_controls": [
                {
                    "group_name": "users",
                    "permission_level": "CAN_READ",
                    "service_principal_name": None,
                    "user_name": None,
                }
            ],
            "dirpath": None,
            "path": "/.laktory/pipelines/pl-dlt/config.json",
            "rootpath": None,
            "source": None,
            "content_base64": "ewogICAgIm5hbWUiOiAicGwtZGx0IiwKICAgICJub2RlcyI6IFsKICAgICAgICB7CiAgICAgICAgICAgICJuYW1lIjogImdsZF9hYiIsCiAgICAgICAgICAgICJzaW5rcyI6IFsKICAgICAgICAgICAgICAgIHsKICAgICAgICAgICAgICAgICAgICAiY2F0YWxvZ19uYW1lIjogInByZCIsCiAgICAgICAgICAgICAgICAgICAgInNjaGVtYV9uYW1lIjogInNhbmRib3gyIiwKICAgICAgICAgICAgICAgICAgICAidGFibGVfbmFtZSI6ICJnbGRfYWIiLAogICAgICAgICAgICAgICAgICAgICJkYXRhZnJhbWVfYmFja2VuZCI6ICJQWVNQQVJLIiwKICAgICAgICAgICAgICAgICAgICAiZGF0YWZyYW1lX2FwaSI6ICJOQVJXSEFMUyIsCiAgICAgICAgICAgICAgICAgICAgImNoZWNrcG9pbnRfcGF0aCI6ICJwaXBlbGluZXMvcGwtZGx0L2dsZF9hYi9jaGVja3BvaW50cy9zaW5rLTVjZGIwNmUxLWNmNDItZTBjZC02YTg5LTg5M2ZjODFkN2NlYSIKICAgICAgICAgICAgICAgIH0KICAgICAgICAgICAgXSwKICAgICAgICAgICAgInRyYW5zZm9ybWVyIjogewogICAgICAgICAgICAgICAgIm5vZGVzIjogWwogICAgICAgICAgICAgICAgICAgIHsKICAgICAgICAgICAgICAgICAgICAgICAgImV4cHIiOiAiU0VMRUNUICogZnJvbSB7bm9kZXMuZ2xkX2F9IFVOSU9OIFNFTEVDVCAqIGZyb20ge25vZGVzLmdsZF9ifSIsCiAgICAgICAgICAgICAgICAgICAgICAgICJkYXRhZnJhbWVfYmFja2VuZCI6ICJQWVNQQVJLIiwKICAgICAgICAgICAgICAgICAgICAgICAgImRhdGFmcmFtZV9hcGkiOiAiTkFSV0hBTFMiCiAgICAgICAgICAgICAgICAgICAgfQogICAgICAgICAgICAgICAgXSwKICAgICAgICAgICAgICAgICJkYXRhZnJhbWVfYmFja2VuZCI6ICJQWVNQQVJLIiwKICAgICAgICAgICAgICAgICJkYXRhZnJhbWVfYXBpIjogIk5BUldIQUxTIgogICAgICAgICAgICB9LAogICAgICAgICAgICAiZGF0YWZyYW1lX2JhY2tlbmQiOiAiUFlTUEFSSyIsCiAgICAgICAgICAgICJkYXRhZnJhbWVfYXBpIjogIk5BUldIQUxTIiwKICAgICAgICAgICAgInJvb3RfcGF0aCI6ICJwaXBlbGluZXMvcGwtZGx0L2dsZF9hYiIsCiAgICAgICAgICAgICJleHBlY3RhdGlvbnNfY2hlY2twb2ludF9wYXRoIjogInBpcGVsaW5lcy9wbC1kbHQvZ2xkX2FiL2NoZWNrcG9pbnRzL2V4cGVjdGF0aW9ucyIKICAgICAgICB9LAogICAgICAgIHsKICAgICAgICAgICAgIm5hbWUiOiAiYnJ6IiwKICAgICAgICAgICAgInNvdXJjZSI6IHsKICAgICAgICAgICAgICAgICJmb3JtYXQiOiAiSlNPTiIsCiAgICAgICAgICAgICAgICAicGF0aCI6ICIvYnJ6X3NvdXJjZS8iLAogICAgICAgICAgICAgICAgImRhdGFmcmFtZV9iYWNrZW5kIjogIlBZU1BBUksiLAogICAgICAgICAgICAgICAgImRhdGFmcmFtZV9hcGkiOiAiTkFSV0hBTFMiLAogICAgICAgICAgICAgICAgInNjaGVtYV9sb2NhdGlvbiI6ICIvIgogICAgICAgICAgICB9LAogICAgICAgICAgICAic2lua3MiOiBbCiAgICAgICAgICAgICAgICB7CiAgICAgICAgICAgICAgICAgICAgImNhdGFsb2dfbmFtZSI6ICJkZXYiLAogICAgICAgICAgICAgICAgICAgICJzY2hlbWFfbmFtZSI6ICJzYW5kYm94IiwKICAgICAgICAgICAgICAgICAgICAidGFibGVfbmFtZSI6ICJicnoiLAogICAgICAgICAgICAgICAgICAgICJkYXRhZnJhbWVfYmFja2VuZCI6ICJQWVNQQVJLIiwKICAgICAgICAgICAgICAgICAgICAiZGF0YWZyYW1lX2FwaSI6ICJOQVJXSEFMUyIsCiAgICAgICAgICAgICAgICAgICAgImNoZWNrcG9pbnRfcGF0aCI6ICJwaXBlbGluZXMvcGwtZGx0L2Jyei9jaGVja3BvaW50cy9zaW5rLWVlZTNlZjBkLWE3ZDEtOTJmOS03MzkxLTc3ZGJjNWRiNGE5ZSIKICAgICAgICAgICAgICAgIH0KICAgICAgICAgICAgXSwKICAgICAgICAgICAgImRhdGFmcmFtZV9iYWNrZW5kIjogIlBZU1BBUksiLAogICAgICAgICAgICAiZGF0YWZyYW1lX2FwaSI6ICJOQVJXSEFMUyIsCiAgICAgICAgICAgICJyb290X3BhdGgiOiAicGlwZWxpbmVzL3BsLWRsdC9icnoiLAogICAgICAgICAgICAiZXhwZWN0YXRpb25zX2NoZWNrcG9pbnRfcGF0aCI6ICJwaXBlbGluZXMvcGwtZGx0L2Jyei9jaGVja3BvaW50cy9leHBlY3RhdGlvbnMiCiAgICAgICAgfSwKICAgICAgICB7CiAgICAgICAgICAgICJuYW1lIjogInNsdiIsCiAgICAgICAgICAgICJzb3VyY2UiOiB7CiAgICAgICAgICAgICAgICAibm9kZV9uYW1lIjogImJyeiIsCiAgICAgICAgICAgICAgICAiZGF0YWZyYW1lX2JhY2tlbmQiOiAiUFlTUEFSSyIsCiAgICAgICAgICAgICAgICAiZGF0YWZyYW1lX2FwaSI6ICJOQVJXSEFMUyIKICAgICAgICAgICAgfSwKICAgICAgICAgICAgInNpbmtzIjogWwogICAgICAgICAgICAgICAgewogICAgICAgICAgICAgICAgICAgICJjYXRhbG9nX25hbWUiOiAiZGV2IiwKICAgICAgICAgICAgICAgICAgICAic2NoZW1hX25hbWUiOiAic2FuZGJveCIsCiAgICAgICAgICAgICAgICAgICAgInRhYmxlX25hbWUiOiAic2x2IiwKICAgICAgICAgICAgICAgICAgICAiZGF0YWZyYW1lX2JhY2tlbmQiOiAiUFlTUEFSSyIsCiAgICAgICAgICAgICAgICAgICAgImRhdGFmcmFtZV9hcGkiOiAiTkFSV0hBTFMiLAogICAgICAgICAgICAgICAgICAgICJjaGVja3BvaW50X3BhdGgiOiAicGlwZWxpbmVzL3BsLWRsdC9zbHYvY2hlY2twb2ludHMvc2luay1hM2ZhYjFhOC00NjI1LThjNTUtMWYxMi0wNWZiNzg2MzdiZjQiCiAgICAgICAgICAgICAgICB9CiAgICAgICAgICAgIF0sCiAgICAgICAgICAgICJ0cmFuc2Zvcm1lciI6IHsKICAgICAgICAgICAgICAgICJub2RlcyI6IFsKICAgICAgICAgICAgICAgICAgICB7CiAgICAgICAgICAgICAgICAgICAgICAgICJmdW5jX2t3YXJncyI6IHsKICAgICAgICAgICAgICAgICAgICAgICAgICAgICJ5MSI6IHsKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAidmFsdWUiOiAieDEiLAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICJkYXRhZnJhbWVfYmFja2VuZCI6ICJQWVNQQVJLIiwKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAiZGF0YWZyYW1lX2FwaSI6ICJOQVJXSEFMUyIKICAgICAgICAgICAgICAgICAgICAgICAgICAgIH0KICAgICAgICAgICAgICAgICAgICAgICAgfSwKICAgICAgICAgICAgICAgICAgICAgICAgImZ1bmNfbmFtZSI6ICJ3aXRoX2NvbHVtbnMiLAogICAgICAgICAgICAgICAgICAgICAgICAiZGF0YWZyYW1lX2JhY2tlbmQiOiAiUFlTUEFSSyIsCiAgICAgICAgICAgICAgICAgICAgICAgICJkYXRhZnJhbWVfYXBpIjogIk5BUldIQUxTIgogICAgICAgICAgICAgICAgICAgIH0sCiAgICAgICAgICAgICAgICAgICAgewogICAgICAgICAgICAgICAgICAgICAgICAiZXhwciI6ICJTRUxFQ1QgaWQsIHgxLCB5MSBmcm9tIHtkZn0iLAogICAgICAgICAgICAgICAgICAgICAgICAiZGF0YWZyYW1lX2JhY2tlbmQiOiAiUFlTUEFSSyIsCiAgICAgICAgICAgICAgICAgICAgICAgICJkYXRhZnJhbWVfYXBpIjogIk5BUldIQUxTIgogICAgICAgICAgICAgICAgICAgIH0KICAgICAgICAgICAgICAgIF0sCiAgICAgICAgICAgICAgICAiZGF0YWZyYW1lX2JhY2tlbmQiOiAiUFlTUEFSSyIsCiAgICAgICAgICAgICAgICAiZGF0YWZyYW1lX2FwaSI6ICJOQVJXSEFMUyIKICAgICAgICAgICAgfSwKICAgICAgICAgICAgImRhdGFmcmFtZV9iYWNrZW5kIjogIlBZU1BBUksiLAogICAgICAgICAgICAiZGF0YWZyYW1lX2FwaSI6ICJOQVJXSEFMUyIsCiAgICAgICAgICAgICJyb290X3BhdGgiOiAicGlwZWxpbmVzL3BsLWRsdC9zbHYiLAogICAgICAgICAgICAiZXhwZWN0YXRpb25zX2NoZWNrcG9pbnRfcGF0aCI6ICJwaXBlbGluZXMvcGwtZGx0L3Nsdi9jaGVja3BvaW50cy9leHBlY3RhdGlvbnMiCiAgICAgICAgfSwKICAgICAgICB7CiAgICAgICAgICAgICJuYW1lIjogImdsZCIsCiAgICAgICAgICAgICJzaW5rcyI6IFsKICAgICAgICAgICAgICAgIHsKICAgICAgICAgICAgICAgICAgICAicGlwZWxpbmVfdmlld19uYW1lIjogImdsZCIsCiAgICAgICAgICAgICAgICAgICAgImRhdGFmcmFtZV9iYWNrZW5kIjogIlBZU1BBUksiLAogICAgICAgICAgICAgICAgICAgICJkYXRhZnJhbWVfYXBpIjogIk5BUldIQUxTIiwKICAgICAgICAgICAgICAgICAgICAiY2hlY2twb2ludF9wYXRoIjogInBpcGVsaW5lcy9wbC1kbHQvZ2xkL2NoZWNrcG9pbnRzL3NpbmstMjRhZjU5MjctM2ZjNy1hNGQ1LTNmYWEtZGFjYjM3YTk5MjAyIgogICAgICAgICAgICAgICAgfQogICAgICAgICAgICBdLAogICAgICAgICAgICAidHJhbnNmb3JtZXIiOiB7CiAgICAgICAgICAgICAgICAibm9kZXMiOiBbCiAgICAgICAgICAgICAgICAgICAgewogICAgICAgICAgICAgICAgICAgICAgICAiZXhwciI6ICJTRUxFQ1QgaWQsIE1BWCh4MSkgQVMgbWF4X3gxIGZyb20ge25vZGVzLnNsdn0gR1JPVVAgQlkgaWQiLAogICAgICAgICAgICAgICAgICAgICAgICAiZGF0YWZyYW1lX2JhY2tlbmQiOiAiUFlTUEFSSyIsCiAgICAgICAgICAgICAgICAgICAgICAgICJkYXRhZnJhbWVfYXBpIjogIk5BUldIQUxTIgogICAgICAgICAgICAgICAgICAgIH0KICAgICAgICAgICAgICAgIF0sCiAgICAgICAgICAgICAgICAiZGF0YWZyYW1lX2JhY2tlbmQiOiAiUFlTUEFSSyIsCiAgICAgICAgICAgICAgICAiZGF0YWZyYW1lX2FwaSI6ICJOQVJXSEFMUyIKICAgICAgICAgICAgfSwKICAgICAgICAgICAgImRhdGFmcmFtZV9iYWNrZW5kIjogIlBZU1BBUksiLAogICAgICAgICAgICAiZGF0YWZyYW1lX2FwaSI6ICJOQVJXSEFMUyIsCiAgICAgICAgICAgICJyb290X3BhdGgiOiAicGlwZWxpbmVzL3BsLWRsdC9nbGQiLAogICAgICAgICAgICAiZXhwZWN0YXRpb25zX2NoZWNrcG9pbnRfcGF0aCI6ICJwaXBlbGluZXMvcGwtZGx0L2dsZC9jaGVja3BvaW50cy9leHBlY3RhdGlvbnMiCiAgICAgICAgfSwKICAgICAgICB7CiAgICAgICAgICAgICJuYW1lIjogImdsZF9hIiwKICAgICAgICAgICAgInNpbmtzIjogWwogICAgICAgICAgICAgICAgewogICAgICAgICAgICAgICAgICAgICJjYXRhbG9nX25hbWUiOiAiZGV2IiwKICAgICAgICAgICAgICAgICAgICAic2NoZW1hX25hbWUiOiAic2FuZGJveDIiLAogICAgICAgICAgICAgICAgICAgICJ0YWJsZV9uYW1lIjogImdsZF9hIiwKICAgICAgICAgICAgICAgICAgICAiZGF0YWZyYW1lX2JhY2tlbmQiOiAiUFlTUEFSSyIsCiAgICAgICAgICAgICAgICAgICAgImRhdGFmcmFtZV9hcGkiOiAiTkFSV0hBTFMiLAogICAgICAgICAgICAgICAgICAgICJjaGVja3BvaW50X3BhdGgiOiAicGlwZWxpbmVzL3BsLWRsdC9nbGRfYS9jaGVja3BvaW50cy9zaW5rLTU2NTJhZTFkLTVkYjEtYTM3NC0yNzNjLTk2ZWI2ZDAwN2IzNiIKICAgICAgICAgICAgICAgIH0KICAgICAgICAgICAgXSwKICAgICAgICAgICAgInRyYW5zZm9ybWVyIjogewogICAgICAgICAgICAgICAgIm5vZGVzIjogWwogICAgICAgICAgICAgICAgICAgIHsKICAgICAgICAgICAgICAgICAgICAgICAgImV4cHIiOiAiU0VMRUNUICogZnJvbSB7bm9kZXMuZ2xkfSBXSEVSRSBpZCA9ICdhJyIsCiAgICAgICAgICAgICAgICAgICAgICAgICJkYXRhZnJhbWVfYmFja2VuZCI6ICJQWVNQQVJLIiwKICAgICAgICAgICAgICAgICAgICAgICAgImRhdGFmcmFtZV9hcGkiOiAiTkFSV0hBTFMiCiAgICAgICAgICAgICAgICAgICAgfQogICAgICAgICAgICAgICAgXSwKICAgICAgICAgICAgICAgICJkYXRhZnJhbWVfYmFja2VuZCI6ICJQWVNQQVJLIiwKICAgICAgICAgICAgICAgICJkYXRhZnJhbWVfYXBpIjogIk5BUldIQUxTIgogICAgICAgICAgICB9LAogICAgICAgICAgICAiZGF0YWZyYW1lX2JhY2tlbmQiOiAiUFlTUEFSSyIsCiAgICAgICAgICAgICJkYXRhZnJhbWVfYXBpIjogIk5BUldIQUxTIiwKICAgICAgICAgICAgInJvb3RfcGF0aCI6ICJwaXBlbGluZXMvcGwtZGx0L2dsZF9hIiwKICAgICAgICAgICAgImV4cGVjdGF0aW9uc19jaGVja3BvaW50X3BhdGgiOiAicGlwZWxpbmVzL3BsLWRsdC9nbGRfYS9jaGVja3BvaW50cy9leHBlY3RhdGlvbnMiCiAgICAgICAgfSwKICAgICAgICB7CiAgICAgICAgICAgICJuYW1lIjogImdsZF9iIiwKICAgICAgICAgICAgInNpbmtzIjogWwogICAgICAgICAgICAgICAgewogICAgICAgICAgICAgICAgICAgICJjYXRhbG9nX25hbWUiOiAiZGV2IiwKICAgICAgICAgICAgICAgICAgICAic2NoZW1hX25hbWUiOiAic2FuZGJveDIiLAogICAgICAgICAgICAgICAgICAgICJ0YWJsZV9uYW1lIjogImdsZF9iIiwKICAgICAgICAgICAgICAgICAgICAiZGF0YWZyYW1lX2JhY2tlbmQiOiAiUFlTUEFSSyIsCiAgICAgICAgICAgICAgICAgICAgImRhdGFmcmFtZV9hcGkiOiAiTkFSV0hBTFMiLAogICAgICAgICAgICAgICAgICAgICJjaGVja3BvaW50X3BhdGgiOiAicGlwZWxpbmVzL3BsLWRsdC9nbGRfYi9jaGVja3BvaW50cy9zaW5rLWNiNmJmMzk4LTljYTEtYmEzMC1kMTRhLWNjYTg1NmYzY2U2YiIKICAgICAgICAgICAgICAgIH0KICAgICAgICAgICAgXSwKICAgICAgICAgICAgInRyYW5zZm9ybWVyIjogewogICAgICAgICAgICAgICAgIm5vZGVzIjogWwogICAgICAgICAgICAgICAgICAgIHsKICAgICAgICAgICAgICAgICAgICAgICAgImV4cHIiOiAiU0VMRUNUICogZnJvbSB7bm9kZXMuZ2xkfSBXSEVSRSBpZCA9ICdiJyIsCiAgICAgICAgICAgICAgICAgICAgICAgICJkYXRhZnJhbWVfYmFja2VuZCI6ICJQWVNQQVJLIiwKICAgICAgICAgICAgICAgICAgICAgICAgImRhdGFmcmFtZV9hcGkiOiAiTkFSV0hBTFMiCiAgICAgICAgICAgICAgICAgICAgfQogICAgICAgICAgICAgICAgXSwKICAgICAgICAgICAgICAgICJkYXRhZnJhbWVfYmFja2VuZCI6ICJQWVNQQVJLIiwKICAgICAgICAgICAgICAgICJkYXRhZnJhbWVfYXBpIjogIk5BUldIQUxTIgogICAgICAgICAgICB9LAogICAgICAgICAgICAiZGF0YWZyYW1lX2JhY2tlbmQiOiAiUFlTUEFSSyIsCiAgICAgICAgICAgICJkYXRhZnJhbWVfYXBpIjogIk5BUldIQUxTIiwKICAgICAgICAgICAgInJvb3RfcGF0aCI6ICJwaXBlbGluZXMvcGwtZGx0L2dsZF9iIiwKICAgICAgICAgICAgImV4cGVjdGF0aW9uc19jaGVja3BvaW50X3BhdGgiOiAicGlwZWxpbmVzL3BsLWRsdC9nbGRfYi9jaGVja3BvaW50cy9leHBlY3RhdGlvbnMiCiAgICAgICAgfQogICAgXSwKICAgICJkYXRhZnJhbWVfYmFja2VuZCI6ICJQWVNQQVJLIiwKICAgICJkYXRhZnJhbWVfYXBpIjogIk5BUldIQUxTIiwKICAgICJyb290X3BhdGgiOiAicGlwZWxpbmVzL3BsLWRsdCIsCiAgICAib3JjaGVzdHJhdG9yIjogewogICAgICAgICJhY2Nlc3NfY29udHJvbHMiOiBbCiAgICAgICAgICAgIHsKICAgICAgICAgICAgICAgICJncm91cF9uYW1lIjogImFjY291bnQgdXNlcnMiLAogICAgICAgICAgICAgICAgInBlcm1pc3Npb25fbGV2ZWwiOiAiQ0FOX1ZJRVciCiAgICAgICAgICAgIH0KICAgICAgICBdLAogICAgICAgICJjYXRhbG9nIjogImRldiIsCiAgICAgICAgIm5hbWUiOiAicGwtZGx0IiwKICAgICAgICAic2NoZW1hXyI6ICJzYW5kYm94IiwKICAgICAgICAidHlwZSI6ICJEQVRBQlJJQ0tTX1BJUEVMSU5FIiwKICAgICAgICAiZGF0YWZyYW1lX2JhY2tlbmQiOiAiUFlTUEFSSyIsCiAgICAgICAgImRhdGFmcmFtZV9hcGkiOiAiTkFSV0hBTFMiCiAgICB9Cn0=",
            "dataframe_backend": "PYSPARK",
            "dataframe_api": "NARWHALS",
        },
        "dataframe_backend": "PYSPARK",
        "dataframe_api": "NARWHALS",
    }

    # Test resources
    resources = pl.core_resources
    assert len(resources) == 4

    dlt = resources[0]
    dltp = resources[1]

    assert isinstance(dlt, models.resources.databricks.Pipeline)
    assert isinstance(dltp, models.resources.databricks.Permissions)

    assert dlt.resource_name == "dlt-pipeline-pl-dlt"
    assert dltp.resource_name == "permissions-dlt-pipeline-pl-dlt"

    assert dlt.options.provider == "${resources.databricks2}"
    assert dltp.options.provider == "${resources.databricks2}"

    assert dlt.options.depends_on == []
    assert dltp.options.depends_on == ["${resources.dlt-pipeline-pl-dlt}"]

    config = dlt.config_file
    print(config.content_dict)
    assert config.content_dict == {
        "name": "pl-dlt",
        "nodes": [
            {
                "name": "gld_ab",
                "sinks": [
                    {
                        "catalog_name": "prd",
                        "schema_name": "sandbox2",
                        "table_name": "gld_ab",
                        "dataframe_backend": "PYSPARK",
                        "dataframe_api": "NARWHALS",
                        "checkpoint_path": "pipelines/pl-dlt/gld_ab/checkpoints/sink-5cdb06e1-cf42-e0cd-6a89-893fc81d7cea",
                    }
                ],
                "transformer": {
                    "nodes": [
                        {
                            "expr": "SELECT * from {nodes.gld_a} UNION SELECT * from {nodes.gld_b}",
                            "dataframe_backend": "PYSPARK",
                            "dataframe_api": "NARWHALS",
                        }
                    ],
                    "dataframe_backend": "PYSPARK",
                    "dataframe_api": "NARWHALS",
                },
                "dataframe_backend": "PYSPARK",
                "dataframe_api": "NARWHALS",
                "root_path": "pipelines/pl-dlt/gld_ab",
                "expectations_checkpoint_path": "pipelines/pl-dlt/gld_ab/checkpoints/expectations",
            },
            {
                "name": "brz",
                "source": {
                    "format": "JSON",
                    "path": "/brz_source/",
                    "dataframe_backend": "PYSPARK",
                    "dataframe_api": "NARWHALS",
                    "schema_location": "/",
                },
                "sinks": [
                    {
                        "catalog_name": "dev",
                        "schema_name": "sandbox",
                        "table_name": "brz",
                        "dataframe_backend": "PYSPARK",
                        "dataframe_api": "NARWHALS",
                        "checkpoint_path": "pipelines/pl-dlt/brz/checkpoints/sink-eee3ef0d-a7d1-92f9-7391-77dbc5db4a9e",
                    }
                ],
                "dataframe_backend": "PYSPARK",
                "dataframe_api": "NARWHALS",
                "root_path": "pipelines/pl-dlt/brz",
                "expectations_checkpoint_path": "pipelines/pl-dlt/brz/checkpoints/expectations",
            },
            {
                "name": "slv",
                "source": {
                    "node_name": "brz",
                    "dataframe_backend": "PYSPARK",
                    "dataframe_api": "NARWHALS",
                },
                "sinks": [
                    {
                        "catalog_name": "dev",
                        "schema_name": "sandbox",
                        "table_name": "slv",
                        "dataframe_backend": "PYSPARK",
                        "dataframe_api": "NARWHALS",
                        "checkpoint_path": "pipelines/pl-dlt/slv/checkpoints/sink-a3fab1a8-4625-8c55-1f12-05fb78637bf4",
                    }
                ],
                "transformer": {
                    "nodes": [
                        {
                            "func_kwargs": {
                                "y1": {
                                    "value": "x1",
                                    "dataframe_backend": "PYSPARK",
                                    "dataframe_api": "NARWHALS",
                                }
                            },
                            "func_name": "with_columns",
                            "dataframe_backend": "PYSPARK",
                            "dataframe_api": "NARWHALS",
                        },
                        {
                            "expr": "SELECT id, x1, y1 from {df}",
                            "dataframe_backend": "PYSPARK",
                            "dataframe_api": "NARWHALS",
                        },
                    ],
                    "dataframe_backend": "PYSPARK",
                    "dataframe_api": "NARWHALS",
                },
                "dataframe_backend": "PYSPARK",
                "dataframe_api": "NARWHALS",
                "root_path": "pipelines/pl-dlt/slv",
                "expectations_checkpoint_path": "pipelines/pl-dlt/slv/checkpoints/expectations",
            },
            {
                "name": "gld",
                "sinks": [
                    {
                        "pipeline_view_name": "gld",
                        "dataframe_backend": "PYSPARK",
                        "dataframe_api": "NARWHALS",
                        "checkpoint_path": "pipelines/pl-dlt/gld/checkpoints/sink-24af5927-3fc7-a4d5-3faa-dacb37a99202",
                    }
                ],
                "transformer": {
                    "nodes": [
                        {
                            "expr": "SELECT id, MAX(x1) AS max_x1 from {nodes.slv} GROUP BY id",
                            "dataframe_backend": "PYSPARK",
                            "dataframe_api": "NARWHALS",
                        }
                    ],
                    "dataframe_backend": "PYSPARK",
                    "dataframe_api": "NARWHALS",
                },
                "dataframe_backend": "PYSPARK",
                "dataframe_api": "NARWHALS",
                "root_path": "pipelines/pl-dlt/gld",
                "expectations_checkpoint_path": "pipelines/pl-dlt/gld/checkpoints/expectations",
            },
            {
                "name": "gld_a",
                "sinks": [
                    {
                        "catalog_name": "dev",
                        "schema_name": "sandbox2",
                        "table_name": "gld_a",
                        "dataframe_backend": "PYSPARK",
                        "dataframe_api": "NARWHALS",
                        "checkpoint_path": "pipelines/pl-dlt/gld_a/checkpoints/sink-5652ae1d-5db1-a374-273c-96eb6d007b36",
                    }
                ],
                "transformer": {
                    "nodes": [
                        {
                            "expr": "SELECT * from {nodes.gld} WHERE id = 'a'",
                            "dataframe_backend": "PYSPARK",
                            "dataframe_api": "NARWHALS",
                        }
                    ],
                    "dataframe_backend": "PYSPARK",
                    "dataframe_api": "NARWHALS",
                },
                "dataframe_backend": "PYSPARK",
                "dataframe_api": "NARWHALS",
                "root_path": "pipelines/pl-dlt/gld_a",
                "expectations_checkpoint_path": "pipelines/pl-dlt/gld_a/checkpoints/expectations",
            },
            {
                "name": "gld_b",
                "sinks": [
                    {
                        "catalog_name": "dev",
                        "schema_name": "sandbox2",
                        "table_name": "gld_b",
                        "dataframe_backend": "PYSPARK",
                        "dataframe_api": "NARWHALS",
                        "checkpoint_path": "pipelines/pl-dlt/gld_b/checkpoints/sink-cb6bf398-9ca1-ba30-d14a-cca856f3ce6b",
                    }
                ],
                "transformer": {
                    "nodes": [
                        {
                            "expr": "SELECT * from {nodes.gld} WHERE id = 'b'",
                            "dataframe_backend": "PYSPARK",
                            "dataframe_api": "NARWHALS",
                        }
                    ],
                    "dataframe_backend": "PYSPARK",
                    "dataframe_api": "NARWHALS",
                },
                "dataframe_backend": "PYSPARK",
                "dataframe_api": "NARWHALS",
                "root_path": "pipelines/pl-dlt/gld_b",
                "expectations_checkpoint_path": "pipelines/pl-dlt/gld_b/checkpoints/expectations",
            },
        ],
        "dataframe_backend": "PYSPARK",
        "dataframe_api": "NARWHALS",
        "root_path": "pipelines/pl-dlt",
        "orchestrator": {
            "access_controls": [
                {"group_name": "account users", "permission_level": "CAN_VIEW"}
            ],
            "catalog": "dev",
            "name": "pl-dlt",
            "schema_": "sandbox",
            "type": "DATABRICKS_PIPELINE",
            "dataframe_backend": "PYSPARK",
            "dataframe_api": "NARWHALS",
        },
    }


def test_databricks_pipeline_execute():
    # TODO
    pass

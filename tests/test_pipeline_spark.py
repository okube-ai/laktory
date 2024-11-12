import os
import io
from pathlib import Path
import shutil
import uuid
import networkx as nx
import pandas as pd
from pyspark.sql import Window
import pyspark.sql.functions as F

from laktory import models
from laktory._testing import spark
from laktory._testing import Paths
from laktory._testing import df_brz

paths = Paths(__file__)

OPEN_FIGURES = False

testdir_path = Path(__file__).parent


def get_pl(clean_path=False):
    pl_path = testdir_path / "tmp" / "test_pipeline_spark" / str(uuid.uuid4())

    with open(os.path.join(paths.data, "pl-spark-local.yaml"), "r") as fp:
        data = fp.read()
        data = data.replace("{data_dir}", str(testdir_path / "data"))
        data = data.replace("{pl_dir}", str(pl_path / "tables"))
        pl = models.Pipeline.model_validate_yaml(io.StringIO(data))
        pl.root_path = pl_path

    if clean_path and os.path.exists(str(pl_path)):
        shutil.rmtree(str(pl_path))

    return pl, pl_path


def get_source(pl_path):
    source_path = str(pl_path / "tables/landing_stock_prices")
    w = Window.orderBy("data.created_at", "data.symbol")
    source = df_brz.withColumn("index", F.row_number().over(w) - 1)

    return source, source_path


gld_target = pd.DataFrame(
    {
        "symbol": ["AAPL", "GOOGL", "MSFT"],
        "max_price": [190.0, 138.0, 330.0],
        "min_price": [170.0, 129.0, 312.0],
        "mean_price": [177.0, 134.0, 320.0],
    }
)


def test_dag():

    pl, _ = get_pl()

    dag = pl.dag

    # Test Dag
    assert nx.is_directed_acyclic_graph(dag)
    assert len(dag.nodes) == 5
    assert len(dag.edges) == 4
    assert list(nx.topological_sort(dag)) == [
        "brz_stock_prices",
        "brz_stock_meta",
        "slv_stock_meta",
        "slv_stock_prices",
        "gld_stock_prices",
    ]

    # Test nodes assignment
    assert [n.name for n in pl.sorted_nodes] == [
        "brz_stock_prices",
        "brz_stock_meta",
        "slv_stock_meta",
        "slv_stock_prices",
        "gld_stock_prices",
    ]
    assert (
        pl.nodes_dict["slv_stock_prices"]
        .transformer.nodes[-1]
        .parsed_func_kwargs["other"]
        .value.node
        == pl.nodes_dict["slv_stock_meta"]
    )

    # Test figure
    fig = pl.dag_figure()
    fig.write_html(os.path.join(paths.tmp, "dag.html"), auto_open=OPEN_FIGURES)


def test_children():

    pl, _ = get_pl()

    for pn in pl.nodes:
        assert pn._parent == pl
        assert pn.source._parent == pn
        if pn.transformer is not None:
            assert pn.transformer._parent == pn
        for s in pn.all_sinks:
            assert s._parent == pn

        if pn.transformer:
            for tn in pn.transformer.nodes:
                assert tn._parent == pn.transformer

        for s in pn.get_sources():
            assert s._parent == pn


def test_paths():
    pl, pl_path = get_pl(clean_path=True)
    assert pl._root_path == pl_path

    for node in pl.nodes:
        assert node._root_path == pl_path / node.name
        assert (
            node._expectations_checkpoint_location
            == pl_path / node.name / "checkpoints" / "expectations"
        )
        for i, s in enumerate(node.all_sinks):
            assert (
                s._checkpoint_location
                == pl_path / node.name / "checkpoints" / f"sink-{s._uuid}"
            )


def test_execute():

    # Get Pipeline
    pl, pl_path = get_pl(clean_path=True)

    # Create Stream Source
    source, source_path = get_source(pl_path)

    # Insert a single row
    source.filter("index=0").write.format("delta").mode("OVERWRITE").save(source_path)
    pl.execute(spark)

    # Test - Brz Stocks
    df = pl.nodes_dict["brz_stock_prices"].primary_sink.read(spark)
    assert df.columns == [
        "name",
        "description",
        "producer",
        "data",
        "index",
        "_bronze_at",
    ]
    assert df.count() == 1

    # Test - Slv Meta
    df = pl.nodes_dict["slv_stock_meta"].output_df
    assert df.columns == ["symbol2", "currency", "first_traded"]
    assert df.count() == 3

    # Test - Slv Stocks
    df = pl.nodes_dict["slv_stock_prices"].primary_sink.read(spark)
    assert df.columns == [
        "_bronze_at",
        "created_at",
        "close",
        "currency",
        "first_traded",
        "symbol",
        "_silver_at",
    ]
    assert df.count() == 1

    # Test - Slv Quarantine
    sink = pl.nodes_dict["slv_stock_prices"].quarantine_sinks[0]
    df = sink.read(spark)
    assert df.count() == 0

    # Test - Gold
    df = pl.nodes_dict["gld_stock_prices"].output_df
    assert df.columns == ["symbol", "max_price", "min_price", "mean_price"]
    assert df.count() == 1

    # Push next rows
    source.filter("index>0 AND index<40").write.format("delta").mode("APPEND").save(
        source_path
    )
    pl.execute(spark)
    source.filter("index>=40").write.format("delta").mode("APPEND").save(source_path)
    pl.execute(spark)

    # Test
    df = pl.nodes_dict["brz_stock_prices"].primary_sink.read(spark)
    assert df.count() == 80
    df = pl.nodes_dict["slv_stock_prices"].primary_sink.read(spark)
    assert df.count() == 52
    df = pl.nodes_dict["slv_stock_prices"].quarantine_sinks[0].read(spark)
    assert df.count() == 8
    df = pl.nodes_dict["slv_stock_meta"].primary_sink.read(spark)
    assert df.count() == 3
    df = pl.nodes_dict["gld_stock_prices"].primary_sink.read(spark).toPandas()
    assert df.round(0).equals(gld_target)

    # Refresh after overwrite
    source.filter("index<40").write.format("delta").mode("OVERWRITE").save(source_path)
    pl.execute(spark, full_refresh=True)

    # Test
    assert pl.nodes_dict["brz_stock_prices"].primary_sink.read(spark).count() == 40
    assert pl.nodes_dict["slv_stock_prices"].primary_sink.read(spark).count() == 22
    assert (
        pl.nodes_dict["slv_stock_prices"].quarantine_sinks[0].read(spark).count() == 8
    )
    assert pl.nodes_dict["slv_stock_meta"].output_df.count() == 3
    assert pl.nodes_dict["gld_stock_prices"].output_df.count() == 3

    # Cleanup
    shutil.rmtree(pl_path)


def test_execute_node():

    # Get Pipeline
    pl, pl_path = get_pl(clean_path=True)

    # Create Stream Source
    source, source_path = get_source(pl_path)

    # Insert 20 rows and execute each node individually
    source.filter("index<40").write.format("delta").mode("OVERWRITE").save(source_path)
    for inode, node in enumerate(pl.sorted_nodes):
        node.execute(spark=spark)
        node._output_df = None

    # Insert remaining rows and execute each node individually
    source.filter("index>=40").write.format("delta").mode("APPEND").save(source_path)
    for inode, node in enumerate(pl.sorted_nodes):
        node.execute(spark=spark)
        node._output_df = None

    # Tests
    assert pl.nodes_dict["brz_stock_prices"].primary_sink.read(spark).count() == 80
    assert pl.nodes_dict["slv_stock_prices"].primary_sink.read(spark).count() == 52
    assert (
        pl.nodes_dict["slv_stock_prices"].quarantine_sinks[0].read(spark).count() == 8
    )
    assert pl.nodes_dict["gld_stock_prices"].primary_sink.read(spark).count() == 3
    df_gld = pl.nodes_dict["gld_stock_prices"].primary_sink.read(spark).toPandas()
    assert df_gld.round(0).equals(gld_target)

    # Cleanup
    shutil.rmtree(pl_path)


def test_sql_join():

    # Get Pipeline
    pl, pl_path = get_pl(clean_path=True)

    # Update join
    node = pl.nodes_dict["slv_stock_prices"]
    t4 = node.transformer.nodes[-1]
    t4.sql_expr = """
    SELECT
        *
    FROM
        {df} as df
    LEFT JOIN
        {nodes.slv_stock_meta} as meta
    ON df.symbol = meta.symbol2
    ;
    """

    # Create Stream Source
    source, source_path = get_source(pl_path)

    # Insert and execute
    source.write.format("delta").mode("OVERWRITE").save(source_path)
    pl.execute(spark)

    # Test
    df = node.primary_sink.read(spark)
    assert df.columns == [
        "_bronze_at",
        "created_at",
        "symbol",
        "close",
        "symbol2",
        "currency",
        "first_traded",
        "_silver_at",
    ]


if __name__ == "__main__":
    test_dag()
    test_children()
    test_paths()
    test_execute()
    test_execute_node()
    test_sql_join()

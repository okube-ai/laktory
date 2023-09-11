from enum import Enum

import os

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Column
from pyspark.java_gateway import ensure_callback_server_started, launch_gateway

from .dataset import ViolationAction

python_graph_load_test = os.getenv('DB_PYTHON_GRAPH_LOAD_TEST')


class Pipeline:
    """
    Pipeline class that can be used to define Delta Live Tables. Pipeline is a Singleton across the
    entire module.
    """
    def __init__(self):
        if python_graph_load_test == "1":
            # When testing graph loading we have to jump through some hoops to attach to
            # the existing session rather than creating a new one.
            self._gateway = launch_gateway()
            self._jvm = self._gateway.jvm
            jsc = self._jvm.org.apache.spark.api.java.JavaSparkContext(
                self._jvm.org.apache.spark.SparkContext.getActive().get())
            sc = SparkContext(master = 'local', appName = 'python connection test', jsc = jsc)
            self._spark = SparkSession(sc)
        else:
            self._spark = SparkSession.builder.getOrCreate()
            self._jvm = self._spark._jvm
            self._gateway = self._spark._sc._gateway
        self._jpipeline = self._jvm.com.databricks.pipelines.execution.core.languages \
            .PythonPipeline.createPythonPipeline()
        ensure_callback_server_started(self._gateway)

    def get_spark_context(self):
        return self._spark

    def get_scala_pipeline(self):
        return self._jpipeline

    def get_gateway(self):
        return self._gateway


class _GraphEntityType(Enum):
    """
    Enum to describe graph entity type.
    """
    FLOW = 1
    DATASET = 2

class _GraphEntity:
    """
    Stores information about a data flow graph entity.
    """
    def __init__(self, name, type):
        self.name = name
        self.type = type

# Contains the Pipeline singleton.
instance = Pipeline()

# Contains the datasets defined in the pipeline. The key is the dataset id and the value is a
# dataset object.
datasets = {}

# Names of all the entities that are defined in the graph in the order they are defined
# along with their type. This is currently used by _DeltaLiveTablesHook to identify and
# resolve delta live table commands defined in a notebook cell.
graph_entities = []

# List of all hooks (EventHook objects) defined in user code.
hooks = []

def _add_graph_entity(entity_name, entity_type):
    """
    Adds given graph entity name and type to a global list.
    :param entity_name: Name of the graph entity.
    :param entity_type: _GraphEntityType
    :return:
    """
    global graph_entities
    graph_entities.append(_GraphEntity(entity_name, entity_type))


def _analyze(analysis_id):
    """
    Analyzes and resolves the datasets defined so far for given analysis_id and returns the
    analyzed result.
    """
    register_expectations()
    dataflow_graph = instance.get_scala_pipeline().toDataflowGraph(False, True)
    return instance._jvm.com.databricks.pipelines.execution.core.SynchronousUpdate.analyze(
        dataflow_graph,
        analysis_id
    )


def register_expectations():
    """
    Adds the user specified expectations to the dataset in the underlying Scala pipeline. We cannot
    add expectations within the expectations decorator, because we may not know which dataset it
    belongs to until the create_table/view decorator is evaluated.
    """
    for _, dataset in datasets.items():
        if dataset.builder is not None and dataset.name_finalized:
            for e in dataset.expectations:
                inv = e.inv._jc if isinstance(e.inv, Column) else e.inv
                if e.action == ViolationAction.ALLOW:
                    dataset.builder = dataset.builder.expect(e.name, inv)
                elif e.action == ViolationAction.FAIL:
                    dataset.builder = dataset.builder.expectOrFail(e.name, inv)
                elif e.action == ViolationAction.DROP:
                    dataset.builder = dataset.builder.expectOrDrop(e.name, inv)
        else:
            raise RuntimeError(
                "Only Delta Live Tables and Views can have expectations. You may be missing a "
                "@view or @table decorator on function {}".format(dataset.name)
            )

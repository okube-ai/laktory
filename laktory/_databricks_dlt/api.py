import types
import warnings
from . import pipeline
from .dataset import Dataset, ViolationAction, Expectation
from .helpers import FlowFunction, EventHook
from inspect import signature
from py4j.java_collections import ListConverter
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Decorators can be chained together. If a decorator is called on a function with this name, we know
# that the user has chained multiple decorators together, and that the function being decorated is
# not the user query function.
DLT_DECORATOR_RETURN = '_dlt_get_dataset'
DLT_HOOK_DECORATOR_RETURN = '_dlt_event_hook'
DLT_DECORATOR_FUNCTION_NAMES = [DLT_HOOK_DECORATOR_RETURN, DLT_DECORATOR_RETURN]

def _sql(stmt, block_unsupported_query_plans):
    """
    Returns an analyzed DataFrame from a SQL statement. The statement can contain DLT specific
    TVFs and datasets which are resolved directly by DLT. This function is called when users run
    spark.sql.
    """
    instance = pipeline.instance
    return DataFrame(
        instance.get_spark_context()._sc._jvm.com.databricks.pipelines.SQLPipelineHelper.sql(
            instance.get_scala_pipeline(), stmt, block_unsupported_query_plans),
        instance.get_spark_context()
    )

def _is_dlt_decorator(func):
    assert callable(func), "Object is not a function"
    assert hasattr(func, '__name__'), "Object does not have name"
    return func.__name__ in DLT_DECORATOR_FUNCTION_NAMES

def _validate_next_chained_decorator_and_get_result(func):
    assert _is_dlt_decorator(func), "Expected chained decorator, received {}".format(func.__class__.__name__)
    assert not func.__name__ == DLT_HOOK_DECORATOR_RETURN, "Hooks cannot be composed with any other DLT decorator."
    dataset = func()
    # if func is indeed a chained decorator, it will return an instance of a dataset. Otherwise if it is a user defined function that was classified as a decorator based on its name, it will not return a dataset instance
    assert isinstance(dataset, Dataset), \
        "The names {} are used by the DLT Runtime. Please rename your dataset".format(DLT_DECORATOR_FUNCTION_NAMES)
    return dataset

def _register_dataset(
        func,
        is_table,
        name,
        comment,
        spark_conf
):
    """
    Creates a Python Dataset, and registers the dataset in the underlying Scala pipeline, with all
    the specified dataset properties.
    """
    # This means we are decorating the user's query function
    if not _is_dlt_decorator(func):
        dataset = Dataset()
        pipeline.datasets[dataset.uid] = dataset
        dataset.func = func
        dataset.name = func.__name__
    # This means decorators have been chained, and we are decorating a DLT API
    else:
        dataset = _validate_next_chained_decorator_and_get_result(func)
    name = dataset.name if name is None else name

    if not isinstance(name, str):
        raise TypeError("Invalid dataset name '{}'. Dataset name must be a string".format(name))

    dataset.name = name
    if dataset.name_finalized:
        raise RuntimeError(
            "Dataset {} already exists. Ensure that the query function has only been marked as a"
            "view or table once".format(dataset)
        )
    dataset.name_finalized = True
    pipeline._add_graph_entity(dataset.name, pipeline._GraphEntityType.DATASET)

    if is_table:
        dataset.builder = (
            pipeline.instance.get_scala_pipeline().createTable(name)
            .query(FlowFunction(dataset.func))
        )
    else:
        dataset.builder = (
            pipeline.instance.get_scala_pipeline().createView(name)
            .query(FlowFunction(dataset.func))
        )
    # Options shared by tables and views
    if comment is not None:
        dataset.builder = dataset.builder.comment(comment)

    if spark_conf is not None:
        for key, value in spark_conf.items():
            dataset.builder = dataset.builder.sparkConf(key, value)
    return dataset


def _register_view(func, name, comment, spark_conf):
    """
    Registers the view in the underlying Scala pipeline with all the specified view properties.
    """
    return _register_dataset(
        func=func,
        is_table=False,
        name=name,
        comment=comment,
        spark_conf=spark_conf)


def _add_table_info(
        table,
        name,
        table_properties,
        partition_cols,
        path,
        schema):
    """
    Adds additional information to the table.

    :param table: The table to add the information to.
    :param name: The name of the table.
    :param table_properties: A dict where the keys are the property names and the values are the
        property values. These properties will be set on the table.
    :param partition_cols: A list containing the column names of the partition columns.
    :param path: The path to the table.
    :param schema Explicit Spark SQL schema to materialize this table with. Supports either a
        Pyspark StructType or a SQL DDL string, such as "a INT, b STRING".
    """
    if table_properties is not None:
        invalid_properties = {key:value for key,value in table_properties.items() if not isinstance(key, str) or not isinstance(value, str)}
        if len(invalid_properties) > 0:
            raise TypeError("Invalid table properties {} for dataset '{}'. The key value pairs for "
                            "a table property must be strings".format(invalid_properties, name))
        for key, value in table_properties.items():
            table = table.tableProperty(key, value)

    if partition_cols is not None:
        if isinstance(partition_cols, str):
            raise TypeError("Invalid partition columns {} for dataset '{}'. Column names must be a collection of "
                            "strings".format(partition_cols, name))
        invalid_cols = [col for col in partition_cols if not isinstance(col, str)]
        if len(invalid_cols) > 0:
            raise TypeError("Invalid partition columns {} for dataset '{}'. Column names must be "
                            "strings".format(invalid_cols, name))

        # Py4J requires arguments to be passed as an array when calling a varags function
        params = pipeline.instance.get_gateway().new_array(
            pipeline.instance.get_gateway().jvm.String, len(partition_cols))
        for i, col in enumerate(partition_cols):
            params[i] = col
        table = table.partitionBy(params)

    if path is not None:
        table = table.path(path)

    if schema is not None:
        if isinstance(schema, str):
            table = table.schema(schema)
        else:
            table = table.schemaJson(schema.json())

    return table


def _register_table(
        func,
        name,
        comment,
        spark_conf,
        table_properties,
        partition_cols,
        path,
        schema,
        temporary):
    """
    Registers the table in the underlying Scala pipeline with all the specified table properties.
    """
    dataset = _register_dataset(
        func=func,
        is_table=True,
        name=name,
        comment=comment,
        spark_conf=spark_conf)
    if temporary is True:
        dataset.builder = dataset.builder.temporary()
    dataset.builder = _add_table_info(dataset.builder, dataset.name, table_properties,
                                               partition_cols,
                                               path,
                                               schema)

    return dataset


def append_flow(name=None,
                target=None,
                comment=None,
                spark_conf=None):
    """
    Return a decorator on a query function to define a flow in a pipeline.

    :param name: The name of the flow. If unspecified, the query function's name will be used.
    :param target: The name of the dataset this flow writes to. Must be specified.
    :param comment: Description of the flow. If unspecified, the dataset's comment will be used.
    :param spark_conf: A dict where the keys are the conf names and the values are the conf values.
        These confs will be set when the flow is executed; they can override confs set for the
        destination, for the pipeline, or on the cluster.
    """
    if target is None:
        raise RuntimeError("Flows must specify their target dataset.")
    if spark_conf is None:
        spark_conf = {}
    def outer(func):
        pipeline.instance.get_scala_pipeline().createTableForTopLevelFlow(target).query(
            name if name is not None else func.__name__,
            FlowFunction(func),
            pipeline.instance.get_spark_context()._sc._jvm.PythonUtils.toScalaMap(spark_conf),
            _to_option_primitive(comment))
    return outer


def table(query_function=None,
          name=None,
          comment=None,
          spark_conf=None,
          table_properties=None,
          partition_cols=None,
          path=None,
          schema=None,
          temporary=False):
    """
    (Return a) decorator to define a table in the pipeline and mark a function as the table's query
    function.

    @table can be used with or without parameters. If called without parameters, Python will
    implicitly pass the decorated query function as the query_function param. If called with
    parameters, @table will return a decorator that is applied on the decorated query function.

    :param query_function: The table's query function. This parameter should not be explicitly
        passed by users. This is passed implicitly by Python if the decorator is called without
        parameters.
    :param name: The name of the dataset. If unspecified, the query function's name will be used.
    :param comment: Description of the dataset.
    :param spark_conf: A dict where the keys are the conf names and the values are the conf values.
        These confs will be set when the query for the dataset is executed and they can override
        confs set for the pipeline or on the cluster.
    :param table_properties: A dict where the keys are the property names and the values are the
        property values. These properties will be set on the table.
    :param partition_cols: A list containing the column names of the partition columns.
    :param path: The path to the table.
    :param schema: Explicit Spark SQL schema to materialize this table with. Supports either a
        Pyspark StructType or a SQL DDL string, such as "a INT, b STRING".
    :param temporary Specifies that this table should be temporary. Temporary tables are excluded
        from being added to the metastore even if a pipeline is configured to add its tables to it.
    """
    def outer(func):
        dataset = _register_table(func=func,
                                  name=name,
                                  comment=comment,
                                  spark_conf=spark_conf,
                                  table_properties=table_properties,
                                  partition_cols=partition_cols,
                                  path=path,
                                  schema=schema,
                                  temporary=temporary)

        def _dlt_get_dataset():
            # If decorators are chained together, this is the function that will be passed to the
            # next decorator.
            return dataset
        return _dlt_get_dataset
    # For a pipeline that looks like this,
    # @create_table(name="tbl")
    # def query_fn():
    #   return ...
    # Python calls create_table(name="tbl")(query_fn). However, we have to also handle the case
    # when @create_table doesn't take any arguments. If a user's pipeline looks like this,
    # @create_table
    # def query_fn():
    #   return ...
    # Python will call create_table(query_fn), so we must call outer and pass query_fn to make it
    # behave the same as the case where the user does specify args.
    if query_function is not None:
        if callable(query_function):
            return outer(query_function)
        else:
            raise RuntimeError(
                "The first positional argument passed to @table must be callable. Either add @table"
                " with no parameters to your query function, or pass options to @table using"
                " keyword arguments (e.g. @table(name='table_a')).")

    else:
        return outer


def view(query_function=None,
         name=None,
         comment=None,
         spark_conf=None):
    """
    (Return a) decorator to define a view in the pipeline and mark a function as the view's query
    function.

    @view can be used with or without parameters. If called without parameters, Python will
    implicitly pass the decorated query function as the query_function param. If called with
    parameters, @view will return a decorator that is applied on the decorated query function.

    :param query_function: The view's query function. This parameter should not be explicitly
        passed by users. This is passed implicitly by Python if the decorator is called without
        parameters.
    :param name: The name of the dataset. If unspecified, the query function's name will be used.
    :param comment: Description of the dataset.
    :param spark_conf: A dict where the keys are the conf names and the values are the conf values.
        These confs will be set when the query for the dataset is executed and they can override
        confs set for the pipeline or on the cluster.
    """
    def outer(func):
        dataset = _register_view(func=func, name=name, comment=comment, spark_conf=spark_conf)

        def _dlt_get_dataset():
            # If decorators are chained together, this is the function that will be passed to the
            # next decorator.
            return dataset
        return _dlt_get_dataset
    # This handles the @create_view case with no args. See similar comment above in create_table.
    if query_function is not None:
        if callable(query_function):
            return outer(query_function)
        else:
            raise RuntimeError(
                "The first positional argument passed to @view must be callable. Either add @view"
                "with no parameters to your query function, or pass options to @view using keyword"
                "arguments (e.g. @view(name='view_a')).")

    else:
        return outer

def on_event_hook(user_event_hook_fn = None, max_allowable_consecutive_failures = None):
    def outer(func):
        assert not _is_dlt_decorator(func), "Hooks cannot be composed with any other DLT decorator."
        assert len(signature(func).parameters) == 1, "Hooks must accept exactly one parameter."
        assert max_allowable_consecutive_failures is None or (isinstance(max_allowable_consecutive_failures, int) and max_allowable_consecutive_failures >= 0), "maxAllowableConsecutiveFailures must be an integer greater than or equal to 0"

        pipeline.hooks.append(EventHook(func, max_allowable_consecutive_failures))

        def _dlt_event_hook(event):
            return func(event)

        return _dlt_event_hook

    if user_event_hook_fn is not None:
        if callable(user_event_hook_fn):
            # Case where user calls @on_event_hook, python will pass along the user event hook to us.
            # While not intended/supported, if the user directly passes in a hook it will be handled here too, which may cause undesirable behavior.
            return outer(user_event_hook_fn)
        else:
            # Case where user doesn't use named arguments for the other parameters, ex. @on_event_hook(5).
            raise RuntimeError(
                "The first positional argument passed to @on_event_hook must be callable. Either add @on_event_hook"
                " with no parameters to your event hook, or pass options to @on_event_hook using"
                " keyword arguments (e.g. @on_event_hook(max_allowable_consecutive_failures=5)).")
    else:
        # Case where user calls @on_event_hook() or @on_event_hook(max_allowable_consecutive_failures = some_integer).
        return outer

def _add_expectations(expectations):
    """
    Adds expectations to the dataset. This is used by the public expectation APIs.
    """
    def outer(func):
        # This means we are decorating the user's query function
        if not _is_dlt_decorator(func):
            dataset = Dataset()
            pipeline.datasets[dataset.uid] = dataset
            dataset.func = func
            dataset.name = func.__name__
        # This means decorators have been chained, and we are decorating a DLT API
        else:
            dataset = _validate_next_chained_decorator_and_get_result(func)
        dataset.expectations.extend(expectations)

        def _dlt_get_dataset():
            return dataset
        return _dlt_get_dataset

    return outer


def expect_all(expectations):
    """
    Adds multiple unenforced expectations on the contents of this dataset. Unexpected records (i.e
    those where the invariant does not evaluate to true) will be counted, but allowed into the
    dataset.

    :param expectations: Dictionary where the key is the expectation name and the value is the
        invariant (string or Column) that will be checked on the dataset.
    """
    return _add_expectations(
        map(lambda kv: Expectation(kv[0], kv[1], ViolationAction.ALLOW), expectations.items()))


def expect_all_or_fail(expectations):
    """
    Adds multiple fail expectations on the contents of this dataset. Unexpected records (i.e those
    where the invariant does not evaluate to true) will cause processing for this and any dependent
    datasets to stop until the problem is addressed.

    :param expectations: Dictionary where the key is the expectation name and the value is the
        invariant (string or Column) that will be checked on the dataset.
    """
    return _add_expectations(
        map(lambda kv: Expectation(kv[0], kv[1], ViolationAction.FAIL), expectations.items()))


def expect_all_or_drop(expectations):
    """
    Adds multiple drop expectations on the contents of this dataset. Unexpected records (i.e those
    where the invariant does not evaluate to true) will be counted and dropped.

    :param expectations: Dictionary where the key is the expectation name and the value is the
        invariant (string or Column) that will be checked on the dataset.
    """
    return _add_expectations(
        map(lambda kv: Expectation(kv[0], kv[1], ViolationAction.DROP), expectations.items()))


def expect(name, inv):
    """
    Adds an unenforced expectation on the contents of this dataset. Unexpected records (i.e those
    where the invariant does not evaluate to true) will be counted, but allowed into the dataset.

    :param name: The name of the expectation
    :param inv: The invariant (string or Column) that will be checked on the dataset
    """
    return _add_expectations([Expectation(name, inv, ViolationAction.ALLOW)])


def expect_or_fail(name, inv):
    """
    Adds a fail expectation on the contents of this dataset. Unexpected records (i.e those where
    the invariant does not evaluate to true) will cause processing for this and any dependent
    datasets to stop until the problem is addressed.

    :param name: The name of the expectation
    :param inv: The invariant (string or Column) that will be checked on the dataset
    """
    return _add_expectations([Expectation(name, inv, ViolationAction.FAIL)])


def expect_or_drop(name, inv):
    """
    Adds a drop expectation on the contents of this dataset. Unexpected records (i.e those where
    the invariant does not evaluate to true) will be counted and dropped.

    :param name: The name of the expectation
    :param inv: The invariant (string or Column) that will be checked on the dataset
    """
    return _add_expectations([Expectation(name, inv, ViolationAction.DROP)])


def read(name):
    """
    Reference another graph component as a complete input.

    :param name: The name of the dataset to read from.
    """
    return DataFrame(
        pipeline.instance.get_scala_pipeline().read(name),
        pipeline.instance.get_spark_context()
    )


def read_stream(name):
    """
    Reference another graph component as an incremental input.

    :param name: The name of the dataset to read from.
    """
    return DataFrame(
        pipeline.instance.get_scala_pipeline().readStream(name),
        pipeline.instance.get_spark_context()
    )


def read_stream_with_reader(name, reader):
    """
    Reference another graph component as an incremental input with a specified DataStreamReader.
    :param name: The name of the dataset to read from.
    """
    return DataFrame(
        pipeline.instance.get_scala_pipeline().readStream(name, reader._jreader),
        pipeline.instance.get_spark_context()
    )


def create_streaming_live_table(
        name,
        comment = None,
        spark_conf = None,
        table_properties=None,
        partition_cols=None,
        path = None,
        schema = None,
        expect_all = None,
        expect_all_or_drop = None,
        expect_all_or_fail = None):
    """
    Creates a target table for Apply Changes (Change Data Capture ingestion). Currently,
    expectations are not supported on the target table.

    Example:
    create_target_table("target")

    :param name: The name of the table.
    :param comment: Description of the table.
    :param spark_conf: A dict where the keys are the conf names and the values are the conf values.
        These confs will be set when the query for the dataset is executed and they can override
        confs set for the pipeline or on the cluster.
    :param table_properties: A dict where the keys are the property names and the values are the
        property values. These properties will be set on the table.
    :param partition_cols: A list containing the column names of the partition columns.
    :param path: The path to the table.
    :param schema Explicit Spark SQL schema to materialize this table with. Supports either a
        Pyspark StructType or a SQL DDL string, such as "a INT, b STRING".

    """
    def add_expectations(table, expectations, action, dataset_name):
        if expectations is None:
            return table
        if not isinstance(expectations, dict):
            raise TypeError("Invalid expectations '{}'. Expectations must be a dict.".format(expectations))
        invalid_properties = {key:value for key,value in expectations.items() if not isinstance(key, str) or not isinstance(value, str)}
        if len(invalid_properties) > 0:
            raise TypeError("Invalid expectations {} for dataset '{}'. The key value pairs for "
                             "an expectation must be strings".format(invalid_properties, dataset_name))
        for exp, inv in expectations.items():
            if action == ViolationAction.ALLOW:
                table = table.expect(exp, inv)
            elif action == ViolationAction.FAIL:
                table = table.expectOrFail(exp, inv)
            elif action == ViolationAction.DROP:
                table = table.expectOrDrop(exp, inv)
        return table

    table = (
        pipeline.instance.get_scala_pipeline().createTable(name)
    )
    pipeline._add_graph_entity(name, pipeline._GraphEntityType.DATASET)

    if comment is not None:
        table = table.comment(comment)
    if spark_conf is not None:
        for key, value in spark_conf.items():
            table = table.sparkConf(key, value)
    table = add_expectations(table, expect_all, ViolationAction.ALLOW, name)
    table = add_expectations(table, expect_all_or_drop, ViolationAction.DROP, name)
    table = add_expectations(table, expect_all_or_fail, ViolationAction.FAIL, name)
    _add_table_info(table, name, table_properties, partition_cols, path, schema)

def __deprecation_warn(func, new_func_name):
    """
    Emits a deprecation warning before running function.

    :param func: The deprecated function to execute.
    :param func: The name of the new function.
    """
    def wrapper(*args, **kwargs):
        warnings.warn("Function {0} has been deprecated. Please use {1} instead.".format(func.__name__, new_func_name))
        func(*args, **kwargs)
    return wrapper

# Deprecated but kept here for backward compatibility. Please use create_streaming_live_table.
create_target_table = __deprecation_warn(create_streaming_live_table, "create_target_table")

class _ScalaDataFrameFunc(object):
    """
    Implements a Scala function that returns a DataFrame. This is used to pass a lambda function
    that returns CDC source to the Scala Apply Changes API.
    """
    def __init__(self, df_func):
        self.df_func = df_func

    def apply(self):
        return self.df_func()._jdf

    class Java:
        implements = ["scala.Function0"]

class _ScalaDataFrameVersionFunc(object):
    """
    Implements a Scala function that takes an argument and returns a tuple of DataFrame and a value
    with Any type.
    This is used to pass a lambda function that takes one argument and returns the tuple of snapshot
    DF and the snapshot version to the Scala Apply Changes FROM SNAPSHOT API.
    """
    def __init__(self, df_func):
        if isinstance(df_func, types.FunctionType):
            self.df_func = df_func
        else:
            self.df_func = lambda v: df_func

    def apply(self, arg):
        # Convert Scala optional argument type to Python value
        if arg == pipeline.instance.get_gateway().jvm.scala.Option.empty():
            arg = None
        else:
            arg = arg.get()

        res = self.df_func(arg)
        if res is None:
            return pipeline.instance.get_gateway().jvm.scala.Option.empty()

        df = res[0]._jdf
        gateway = pipeline.instance.get_spark_context()._sc._jvm._gateway_client
        return pipeline.instance.get_gateway().jvm.scala.Option.apply(
            ListConverter().convert([df, res[1]], gateway)
        )

    class Java:
        implements = ["scala.Function1"]


def _to_column(identifier):
    """
    Converts a Python string or Pyspark Column to a Spark Column.
    """
    if isinstance(identifier, str):
        return F.col(identifier)._jc
    return identifier._jc

def _to_expr(input):
    """
    Converts a Python string or Pyspark Expression to a Spark column.
    """
    if isinstance(input, str):
        return F.expr(input)._jc
    return input._jc

def _to_seq_columns(vals):
    """
    Converts a Python list (of either strings or Pyspark Columns) to Scala Seq of Spark Columns.
    """
    l = pipeline.instance.get_gateway().jvm.java.util.ArrayList()
    if vals is not None:
        for val in vals:
            l.append(_to_column(val))
    return pipeline.instance.get_gateway().jvm.scala.collection.JavaConversions.asScalaBuffer(l).toSeq()


def _to_option_column(val):
    """
    Converts a Pyspark Column to Scala Option of Spark Column.
    """
    if val is None:
        return pipeline.instance.get_gateway().jvm.scala.Option.empty()
    return pipeline.instance.get_gateway().jvm.scala.Option.apply(val._jc)

def _to_option_primitive(val):
    """
    Converts a Pyspark primitive to a Scala option.
    """
    if val is None:
        return pipeline.instance.get_gateway().jvm.scala.Option.empty()
    return pipeline.instance.get_gateway().jvm.scala.Option.apply(val)

def _to_option_seq_columns(val):
    """
    Converts None-able Python list (of either strings or Pyspark Columns) to Scala Option of Seq
    of Spark Columns.
    """
    if val is None:
        return pipeline.instance.get_gateway().jvm.scala.Option.empty()
    return pipeline.instance.get_gateway().jvm.scala.Option.apply(_to_seq_columns(val))

def apply_changes(
        target,
        source,
        keys,
        sequence_by,
        where = None,
        ignore_null_updates = False,
        apply_as_deletes = None,
        apply_as_truncates = None,
        column_list = None,
        except_column_list = None,
        stored_as_scd_type = "1",
        track_history_column_list = None,
        track_history_except_column_list = None):
    """
    Apply changes into the target table from the Change Data Capture (CDC) source. Target table must
    have already been created using create_target_table function. Only one of column_list and
    except_column_list can be specified. Also only one of track_history_column_list and
    track_history_except_column_list can be specified.

    Example:
    apply_changes(
      target = "target",
      source = "source",
      keys = ["key"],
      sequence_by = "sequence_expr",
      ignore_null_updates = True,
      column_list = ["key", "value"],
      stored_as_scd_type = "1",
      track_history_column_list = ["value"]
    )

    Note that for keys, sequence_by, column_list, except_column_list, track_history_column_list,
    track_history_except_column_list the arguments have to be column identifiers without qualifiers,
    e.g. they cannot be col("sourceTable.keyId").

    :param target: The name of the target table that receives the Apply Changes.
    :param source: The name of the CDC source to stream from.
    :param keys: The column or combination of columns that uniquely identify a row in the source
        data. This is used to identify which CDC events apply to specific records in the target
        table. These keys also identify records in the target table, e.g., if there exists a record
        for given keys and the CDC source has an UPSERT operation for the same keys, we will update
        the existing record. At least one key must be provided. This should be a list of column
        identifiers without qualifiers, expressed as either Python strings or Pyspark Columns.
    :param sequencing_by: An expression that we use to order the source data. This can be expressed
        as either a Python string or Pyspark Expression.
    :param where: An optional condition applied to both source and target during the execution
        process to trigger optimizations such as partition pruning. This condition cannot be used to
        drop source rows; that is, all CDC rows in the source must satisfy this condition, or an
        error is thrown.
    :param ignore_null_updates: Whether to ignore the null value in the source data. For example,
        consider the case where we have an UPSERT in the source data with null value for a column,
        and this same column has non-null value in the target. If ignore_null_updates is true,
        merging this UPSERT will not override the existing value for this column; If false,
        merging will override the value for this column to null.
    :param apply_as_deletes: Delete condition for the merged operation. This should be a string of
        expression e.g. "operation = 'DELETE'"
    :param apply_as_truncates: Truncate condition for the merged operation. This should be a string
        expression e.g. "operation = 'TRUNCATE'"
    :param column_list: Columns that will be included in the output table. This should be a list
        of column identifiers without qualifiers, expressed as either Python strings or Pyspark
        Column. Only one of column_list and except_column_list can be specified.
    :param except_column_list: Columns that will be excluded in the output table. This should be a
        list of column identifiers without qualifiers, expressed as either Python strings or Pyspark
        Column. Only one of column_list and except_column_list can be specified. When this is
        specified, all columns in the dataframe of the target table except those in this list will
        be in the output table.
    :param stored_as_scd_type: Specify the SCD Type for output format. 1 for SCD Type 1 and 2 for
        SCD Type 2. This parameter can either be an integer or string. Default value is 1.
    :param track_history_column_list: Columns that will be tracked for change history.
        This should be a list of column identifiers without qualifiers, expressed as either Python
        strings or Pyspark Column. Only one of track_history_column_list and
        track_history_except_column_list can be specified.
    :param track_history_except_column_list: Columns that will not be tracked for change history.
        Those columns will reflect the values that were seen before the next update to any of the
        tracked columns.
        This should be a list of column identifiers without qualifiers, expressed as either Python
        strings or Pyspark Column. Only one of track_history_column_list and
        track_history_except_column_list can be specified.
    """

    if isinstance(apply_as_deletes, str):
        apply_as_deletes = F.expr(apply_as_deletes)

    if isinstance(apply_as_truncates, str):
        apply_as_truncates = F.expr(apply_as_truncates)

    if isinstance(where, str):
        where = F.expr(where)

    stored_as_scd_type = str(stored_as_scd_type)
    if stored_as_scd_type not in ("1", "2"):
        raise RuntimeError("Only SCD Type 1 and SCD Type 2 are supported for now.")

    jvm = pipeline.instance.get_gateway().jvm
    if stored_as_scd_type == "1":
        stored_as_scd_type = jvm.com.databricks.pipelines.graph.SCD_TYPE.SCD_TYPE1()
    elif stored_as_scd_type == "2":
        stored_as_scd_type = jvm.com.databricks.pipelines.graph.SCD_TYPE.SCD_TYPE2()
    else:
        raise RuntimeError("Only SCD Type 1 and SCD Type 2 are supported for now.")

    pipeline._add_graph_entity(target, pipeline._GraphEntityType.FLOW)
    pipeline.instance.get_scala_pipeline().createTableForTopLevelFlow(target).changeQuery(
      _ScalaDataFrameFunc(lambda: read_stream(source)),
      _to_seq_columns(keys),
      _to_expr(sequence_by),
      ignore_null_updates,
      _to_seq_columns(except_column_list),
      _to_option_column(apply_as_deletes),
      _to_option_column(apply_as_truncates),
      _to_seq_columns(column_list),
      _to_option_column(where), # extraMergeCondition
      stored_as_scd_type,
      _to_seq_columns(track_history_column_list),
      _to_seq_columns(track_history_except_column_list)
    )


def apply_changes_from_snapshot(
        target,
        snapshot_and_version,
        keys,
        stored_as_scd_type,
        track_history_column_list = None,
        track_history_except_column_list = None):
    """
    APPLY CHANGES into the target table from a sequence of snapshots. Target table must
    have already been created using create_target_table function. Only one of
    track_history_column_list and track_history_except_column_list can be specified with SCD TYPE 2.

    Example:
    def next_snapshot_and_version(latest_snapshot_version):
     version = latest_snapshot_version + 1
     file_name = "dir_path/filename_" + version + ".<file_format>"
     return (spark.read.load(file_name) if(exist(file_name)) else None, version)

    apply_changes_from_snapshot(
      target = "target",
      track_history_column_list = next_snapshot_and_version,
      keys = ["key"],
      stored_as_scd_type = "1",
      track_history_column_list = ["value"]
    )

    :param target: The name of the target table that receives the APPLY CHANGES.
    :param snapshot_and_version: The lambda function that takes latest process snapshot version as
        an argument and optionally return the tuple of new snapshot DF to be processed and the
        version of the snapshot. Each time the apply_changes_from_snapshot pipeline get triggered,
        we will keep execute `snapshot_and_version` to get the next new snapshot DF to process until
        there is no snapshot DF returned from the function.
    :param keys: The column or combination of columns that uniquely identify a row in the source
        data. This is used to identify which CDC events apply to specific records in the target
        table. These keys also identify records in the target table, e.g., if there exists a record
        for given keys and the CDC source has an UPSERT operation for the same keys, we will update
        the existing record. At least one key must be provided. This should be a list of column
        identifiers without qualifiers, expressed as either Python strings or Pyspark Columns.
    :param stored_as_scd_type: Specify the SCD Type for output format. 1 for SCD Type 1 and 2 for
        SCD Type 2. This parameter can either be an integer or string. Default value is 1.
    :param track_history_column_list: Columns that will be tracked for change history.
        This should be a list of column identifiers without qualifiers, expressed as either Python
        strings or Pyspark Column. Only one of track_history_column_list and
        track_history_except_column_list can be specified.
    :param track_history_except_column_list: Columns that will not be tracked for change history.
        Those columns will reflect the values that were seen before the next update to any of the
        tracked columns.
        This should be a list of column identifiers without qualifiers, expressed as either Python
        strings or Pyspark Column. Only one of track_history_column_list and
        track_history_except_column_list can be specified.
    """
    stored_as_scd_type = str(stored_as_scd_type)
    jvm = pipeline.instance.get_gateway().jvm
    if stored_as_scd_type == "1":
        stored_as_scd_type = jvm.com.databricks.pipelines.graph.SCD_TYPE.SCD_TYPE1()
    elif stored_as_scd_type == "2":
        stored_as_scd_type = jvm.com.databricks.pipelines.graph.SCD_TYPE.SCD_TYPE2()
    else:
        raise RuntimeError("Only SCD Type 1 and SCD Type 2 are supported for now.")

    # if user passed a non-function as snapshot_and_version from api, we mark the `process_once`
    # flag to make sure only execute `snapshotAndVersionLambda` function once.
    process_once = not isinstance(snapshot_and_version, types.FunctionType)

    pipeline._add_graph_entity(target, pipeline._GraphEntityType.FLOW)
    pipeline.instance.get_scala_pipeline().createTableForTopLevelFlow(target).snapshotChangeQuery(
        _ScalaDataFrameVersionFunc(snapshot_and_version),
        _to_seq_columns(keys),
        stored_as_scd_type,
        _to_option_seq_columns(track_history_column_list),
        _to_option_seq_columns(track_history_except_column_list),
        process_once
    )

def create_feature_table(
        name,
        comment = None,
        primary_keys = None,
        timestamp_key = None,
        spark_conf = None,
        table_properties=None,
        partition_cols=None,
        path = None,
        schema = None):
    """
    Creates a feature table in the Feature Store to use with Delta Live Tables Change Data Capture
    and apply_changes() function. Expectations are not supported on the feature table.

    Example:
    create_feature_table("target")

    :param name: The name of the table.
    :param comment: Description of the table.
    :param primary_keys: The feature table's primary keys. The timestamp key and primary keys of the
        feature table uniquely identify the feature value for an entity at a point in time.
    :param timestamp_key: Column containing the event time associated with feature value.
        The timestamp key and primary keys of the feature table uniquely identify the feature value
        for an entity at a point in time.
    :param spark_conf: A dict where the keys are the configuration names and the values are the configuration values.
        These configurations will be set when the query for the dataset is executed and they can override
        configurations set for the pipeline or on the cluster.
    :param table_properties: A dict where the keys are the property names and the values are the
        property values. These properties will be set on the table.
    :param partition_cols: A list containing the column names of the partition columns.
    :param path: The path to the table.
    :param schema: Explicit Spark SQL schema to materialize this table with. Supports either a
        Pyspark StructType or a SQL DDL string, such as "a INT, b STRING".
    """
    table = (
        pipeline.instance.get_scala_pipeline().createTable(name)
    )

    if comment is not None:
        table = table.comment(comment)
    if spark_conf is not None:
        for key, value in spark_conf.items():
            table = table.sparkConf(key, value)

    if primary_keys is None or timestamp_key is None:
        raise RuntimeError("Both primary_keys and timestamp_key must be defined for feature tables in DLT.")

    table = table.featureTableProperties(_to_seq_columns(primary_keys), _to_expr(timestamp_key))

    _add_table_info(table, name, table_properties, partition_cols, path, schema)

"""
Decorators for registering custom namespaces on Narwhals and PySpark objects.

These utilities let users extend Laktory pipelines with custom transformation
logic without modifying Laktory itself. Exposed publicly via ``laktory.api``.
"""

import narwhals as nw

# --------------------------------------------------------------------------- #
# Descriptors                                                                  #
# --------------------------------------------------------------------------- #


class NameSpace:
    def __init__(self, name: str, ns_cls: type):
        self._name = name
        self._ns_cls = ns_cls

    def __get__(self, instance, owner):
        if instance is None:
            return self._ns_cls
        ns_instance = self._ns_cls(instance)
        setattr(instance, self._name, ns_instance)
        return ns_instance


class SparkNameSpace:
    """
    Non-caching descriptor for PySpark objects.

    Unlike `NameSpace`, this version does not cache the namespace instance on
    the object to avoid interfering with PySpark's own ``__getattr__`` on
    ``DataFrame`` and ``Column``.
    """

    def __init__(self, name: str, ns_cls: type):
        self._name = name
        self._ns_cls = ns_cls

    def __get__(self, instance, owner):
        if instance is None:
            return self._ns_cls
        return self._ns_cls(instance)


# --------------------------------------------------------------------------- #
# Narwhals registration                                                        #
# --------------------------------------------------------------------------- #


def register_expr_namespace(name: str):
    """
    Decorator for registering a custom namespace on a Narwhals Expression.

    Parameters
    ----------
    name:
        Name of the namespace.

    Examples
    -------
    ```py
    import narwhals as nw
    import polars as pl

    import laktory as lk


    @lk.api.register_expr_namespace("custom")
    class CustomNamespace:
        def __init__(self, _expr):
            self._expr = _expr

        def double(self):
            return self._expr * 2


    df = nw.from_native(pl.DataFrame({"x": [0, 1]}))

    df = df.with_columns(x2=nw.col("x").custom.double())

    print(df)
    '''
    ┌──────────────────┐
    |Narwhals DataFrame|
    |------------------|
    |    | x | x2 |    |
    |    |---|----|    |
    |    | 0 | 0  |    |
    |    | 1 | 2  |    |
    └──────────────────┘
    '''
    ```

    References
    ----------
    * [Narwhals Extension](https://www.laktory.ai/concepts/extension_custom/)
    """

    def wrapper(ns_cls: type):
        setattr(nw.Expr, name, NameSpace(name, ns_cls))
        return ns_cls

    return wrapper


def register_anyframe_namespace(name: str):
    """
    Decorator for registering a custom namespace on a Narwhals DataFrame and LazyFrame.

    Parameters
    ----------
    name:
        Name of the namespace.

    Examples
    -------
    ```py
    import narwhals as nw
    import polars as pl

    import laktory as lk


    @lk.api.register_anyframe_namespace("custom")
    class CustomNamespace:
        def __init__(self, _df):
            self._df = _df

        def with_x2(self):
            return self._df.with_columns(x2=nw.col("x") * 2)


    df = nw.from_native(pl.DataFrame({"x": [0, 1]}))

    df = df.custom.with_x2()

    print(df)
    '''
    ┌──────────────────┐
    |Narwhals DataFrame|
    |------------------|
    |    | x | x2 |    |
    |    |---|----|    |
    |    | 0 | 0  |    |
    |    | 1 | 2  |    |
    └──────────────────┘
    '''
    ```

    References
    ----------
    * [Narwhals Extension](https://www.laktory.ai/concepts/extension_custom/)
    """

    def wrapper(ns_cls: type):
        setattr(nw.DataFrame, name, NameSpace(name, ns_cls))
        setattr(nw.LazyFrame, name, NameSpace(name, ns_cls))
        return ns_cls

    return wrapper


def register_dataframe_namespace(name: str):
    """
    Decorator for registering a custom namespace on a Narwhals DataFrame.

    Parameters
    ----------
    name:
        Name of the namespace.

    Examples
    -------
    ```py
    import narwhals as nw
    import polars as pl

    import laktory as lk


    @lk.api.register_dataframe_namespace("custom")
    class CustomNamespace:
        def __init__(self, _df):
            self._df = _df

        def with_x2(self):
            return self._df.with_columns(x2=nw.col("x") * 2)


    df = nw.from_native(pl.DataFrame({"x": [0, 1]}))

    df = df.custom.with_x2()

    print(df)
    '''
    ┌──────────────────┐
    |Narwhals DataFrame|
    |------------------|
    |    | x | x2 |    |
    |    |---|----|    |
    |    | 0 | 0  |    |
    |    | 1 | 2  |    |
    └──────────────────┘
    '''
    ```

    References
    ----------
    * [Narwhals Extension](https://www.laktory.ai/concepts/extension_custom/)
    """

    def wrapper(ns_cls: type):
        setattr(nw.DataFrame, name, NameSpace(name, ns_cls))
        return ns_cls

    return wrapper


def register_lazyframe_namespace(name: str):
    """
    Decorator for registering a custom namespace on a Narwhals LazyFrame.

    Parameters
    ----------
    name:
        Name of the namespace.

    Examples
    -------
    ```py
    import narwhals as nw
    import polars as pl

    import laktory as lk


    @lk.api.register_lazyframe_namespace("custom")
    class CustomNamespace:
        def __init__(self, _df):
            self._df = _df

        def with_x2(self):
            return self._df.with_columns(x2=nw.col("x") * 2)


    df = nw.from_native(pl.DataFrame({"x": [0, 1]}).lazy())

    df = df.custom.with_x2()

    print(df.collect().sort("x"))
    '''
    ┌──────────────────┐
    |Narwhals DataFrame|
    |------------------|
    |    | x | x2 |    |
    |    |---|----|    |
    |    | 0 | 0  |    |
    |    | 1 | 2  |    |
    └──────────────────┘
    '''
    ```

    References
    ----------
    * [Narwhals Extension](https://www.laktory.ai/concepts/extension_custom/)
    """

    def wrapper(ns_cls: type):
        setattr(nw.LazyFrame, name, NameSpace(name, ns_cls))
        return ns_cls

    return wrapper


# --------------------------------------------------------------------------- #
# Spark registration                                                           #
# --------------------------------------------------------------------------- #


def register_spark_dataframe_namespace(name: str):
    """
    Decorator for registering a custom namespace on PySpark's ``DataFrame`` class.

    The decorated class receives the **native PySpark DataFrame** as its first
    ``__init__`` argument, so its methods are written in pure PySpark without
    any Narwhals knowledge.

    A thin bridge is automatically registered on Narwhals ``DataFrame`` and
    ``LazyFrame`` as well, so the same namespace works with both
    ``DATAFRAME_API=NATIVE`` and ``DATAFRAME_API=NARWHALS`` in pipeline YAML.

    Parameters
    ----------
    name:
        Namespace name, used as ``func_name: "<name>.<method>"`` in YAML.

    Examples
    --------
    ```py
    import pyspark.sql.functions as F
    import laktory as lk


    @lk.api.register_spark_dataframe_namespace("custom")
    class CustomOps:
        def __init__(self, _df):
            self._df = _df

        def with_x2(self):
            return self._df.withColumn("x2", F.col("x1") * 2)
    ```

    Use in a pipeline YAML (both APIs work):

    ```yaml
    transformer:
      nodes:
        - func_name: custom.with_x2
          dataframe_api: NATIVE    # calls PySpark DataFrame directly

        - func_name: custom.with_x2
          dataframe_api: NARWHALS  # auto-bridged: nw.LazyFrame → native → nw.LazyFrame
    ```

    References
    ----------
    * [Spark Extension](https://www.laktory.ai/concepts/spark_extension/)
    """

    def wrapper(ns_cls: type):
        # NATIVE mode: patch PySpark DataFrame directly
        from pyspark.sql import DataFrame as SparkDataFrame

        setattr(SparkDataFrame, name, SparkNameSpace(name, ns_cls))

        # NARWHALS mode: auto-bridge via a proxy class
        class _NarwhalsBridge:
            """
            Proxy that holds a Narwhals frame, creates the Spark namespace on the
            underlying native PySpark DataFrame, and forwards method calls while
            wrapping the result back into Narwhals.
            """

            def __init__(self, _df):
                self._ns = ns_cls(_df.to_native())

            def __getattr__(self, attr_name: str):
                method = getattr(self._ns, attr_name)
                if not callable(method):
                    return method

                def _wrapped(*args, **kwargs):
                    result = method(*args, **kwargs)
                    if not isinstance(result, (nw.DataFrame, nw.LazyFrame)):
                        try:
                            result = nw.from_native(result)
                        except TypeError:
                            pass
                    return result

                return _wrapped

        setattr(nw.DataFrame, name, NameSpace(name, _NarwhalsBridge))
        setattr(nw.LazyFrame, name, NameSpace(name, _NarwhalsBridge))

        return ns_cls

    return wrapper


def register_spark_column_namespace(name: str):
    """
    Decorator for registering a custom namespace on PySpark's ``Column`` class.

    The decorated class receives a **native PySpark Column** as its first
    ``__init__`` argument. Use this to build reusable column expressions that
    can be referenced from ``func_args`` strings when ``DATAFRAME_API=NATIVE``.

    .. note::
        This namespace is NATIVE-only. A Narwhals ``Expr`` bridge is not
        provided because Narwhals expressions and PySpark Columns have
        incompatible semantics.

    Parameters
    ----------
    name:
        Namespace name, accessible as ``col("x").name.method()`` inside
        evaluated ``func_args`` strings.

    Examples
    --------
    ```py
    import laktory as lk


    @lk.api.register_spark_column_namespace("custom")
    class CustomColOps:
        def __init__(self, _col):
            self._col = _col

        def double(self):
            return self._col * 2
    ```

    Use in a pipeline YAML:

    ```yaml
    transformer:
      nodes:
        - func_name: withColumn
          func_args:
            - x2
            - "col('x1').custom.double()"
          dataframe_api: NATIVE
    ```

    References
    ----------
    * [Spark Extension](https://www.laktory.ai/concepts/spark_extension/)
    """

    def wrapper(ns_cls: type):
        from pyspark.sql.column import Column

        setattr(Column, name, SparkNameSpace(name, ns_cls))
        return ns_cls

    return wrapper

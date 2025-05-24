from laktory._registries.funcsregistry import FuncsRegistry


def func(namespace: str = None, name: str = None):
    """
    Decorator for registering a custom DataFrame transformation function. Once the
    function is registered, it can be used as a node in a DataFrameTransformer
    method.

    Parameters
    ----------
    namespace:
        Namespace in which the function is registered.
    name:
        Name used to register the function. If `None` is provided, the function
        `__name__` is used instead.

    Examples
    --------
    ```py
    import laktory.models
    import laktory.custom


    @custom.func()
    def select2(df, cols):
        return df.select(cols)


    df0 = get_df0(backend)
    node = DataFrameMethod(
        func_name="select2",
        func_args=[["id", "x1"]],
    )
    df = node.execute(df0)
    ```

    Returns
    -------
    Registered function
    """

    def decorator(func):
        FuncsRegistry().register(func=func, namespace=namespace, name=name)
        return func

    return decorator

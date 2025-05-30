import narwhals as nw


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


def register_expr_namespace(name: str):
    def wrapper(ns_cls: type):
        setattr(nw.Expr, name, NameSpace(name, ns_cls))
        return ns_cls

    return wrapper


def register_dataframe_namespace(name: str):
    def wrapper(ns_cls: type):
        setattr(nw.DataFrame, name, NameSpace(name, ns_cls))
        return ns_cls

    return wrapper


def register_lazyframe_namespace(name: str):
    def wrapper(ns_cls: type):
        setattr(nw.LazyFrame, name, NameSpace(name, ns_cls))
        return ns_cls

    return wrapper

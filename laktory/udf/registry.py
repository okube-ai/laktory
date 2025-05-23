import threading
from typing import Any
from typing import Callable


class UDFRegistry:
    _instance = None
    _lock = threading.Lock()

    def __init__(self):
        if not hasattr(self, "_udfs"):
            self._udfs: dict[str, Callable[..., Any]] = {}

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(UDFRegistry, cls).__new__(cls)
        return cls._instance

    def register(self, func: Callable, namespace: str = None, name: str = None):
        if name is None:
            name = func.__name__

        key = name
        if namespace:
            key = namespace + "." + key

        self._udfs[key] = func

    def get(self, name, namespace: str = None):
        key = name
        if namespace:
            key = namespace + "." + key
        if key not in self._udfs:
            raise KeyError(f"Key '{key}' is not available for UDF registry.")

        return self._udfs[key]


def register_udf(namespace: str = None, name: str = None):
    def decorator(func):
        UDFRegistry().register(func=func, namespace=namespace, name=name)
        return func

    return decorator

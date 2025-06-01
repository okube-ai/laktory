import threading
from typing import Any
from typing import Callable


class FuncsRegistry:
    _instance = None
    _lock = threading.Lock()

    def __init__(self):
        if not hasattr(self, "_items"):
            self._items: dict[str, Callable[..., Any]] = {}

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(FuncsRegistry, cls).__new__(cls)
        return cls._instance

    def register(self, func: Callable, namespace: str = None, name: str = None):
        if name is None:
            name = func.__name__

        key = name
        if namespace:
            key = namespace + "." + key

        self._items[key] = func

    def get(self, name, namespace: str = None):
        key = name
        if namespace:
            key = namespace + "." + key
        if key not in self._items:
            raise KeyError(f"UDF with name '{key}' is not available for UDF registry.")

        return self._items[key]

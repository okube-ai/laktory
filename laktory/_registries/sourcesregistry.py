from __future__ import annotations

import threading


class SourcesRegistry:
    _instance = None
    _lock = threading.Lock()

    def __init__(self):
        raise NotImplementedError()


def source(namespace: str = None, name: str = None):
    def decorator(func):
        SourcesRegistry().register(func=func, namespace=namespace, name=name)
        return func

    return decorator

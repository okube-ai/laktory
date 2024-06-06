from functools import wraps
import inspect

def func1(x: float, y: int = 0):
# def func1(x, y):
    """test"""
    return x + y


@wraps(func1)
def func2(*args, **kwargs):
    return func1(*args, **kwargs)

# func2.__doc__ = func1.__doc__
# func2.__annotations__ = func1.__annotations__


print(inspect.getdoc(func1))
print(inspect.signature(func1))
print()
print(inspect.getdoc(func2))
print(inspect.signature(func2))

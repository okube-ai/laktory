from laktory import models


class MyModel(models.BaseModel):
    f_int: int = None
    f_float: float = None
    f_str: str = None
    f_bool: str = None


# Example usage
model = MyModel(f_int=5)
print(model.model_dump())

import pydantic


class Model(pydantic.BaseModel):
    a: int

    # @pydantic.computed_field(repr=False)
    @property
    def b(self) -> int:
        return self.a + 1


m = Model(a=5)

print(m)
print(m.model_dump())

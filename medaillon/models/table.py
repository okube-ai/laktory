from pydantic import BaseModel

from medaillon.models.column import Column


class Table(BaseModel):
    name: str
    comment: str = None
    columns: list[Column] = []


if __name__ == "__main__":
    speed = Column(
        name="airspeed",
        type="double",
        test=0,
    )

    altitude = Column(
        name="altitude",
        type="double",
        test=0,
    )

    table = Table(
        name="takeoff",
        columns=[speed, altitude]
    )




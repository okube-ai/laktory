import polars as pl
from datetime import datetime

df = pl.DataFrame(
    {
        "i": [1, 2, 3],
        "d": [
            datetime(2025, 1, 1),
            datetime(2025, 1, 2),
            datetime(2025, 1, 3),
        ],
        "f": [4.0, 5.0, 6.0],
        "s": ["a", "b", "c"],
    }
)

print(df)

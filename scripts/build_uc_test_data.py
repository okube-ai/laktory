import polars as pl
import numpy as np

# Parameters
table_name = "sin"
n = 100
ids = [0, 1, 2]
start_date = np.datetime64("2010-01-01T00:00", "h")

# Generate base timestamp series with hourly intervals
tstamps = start_date + np.arange(n, dtype="timedelta64[h]")
tstamps = tstamps.astype("datetime64[ns]")

dfs = []
for id_val in ids:
    # Generate sine/cosine signals
    x = np.sin(np.linspace(0, 50 * np.pi, n))
    x = (x + 1) / 2 * 100  # scale to [0,100]

    y = np.cos(np.linspace(0, 50 * np.pi, n)) * 100  # scale to [-100,100]

    # Insert ~2% nulls
    mask_x = np.random.rand(n) < 0.02
    mask_y = np.random.rand(n) < 0.02
    x[mask_x] = np.nan
    y[mask_y] = np.nan

    # Categories
    c = np.random.choice(["a", "b", "c", "d", "e"], size=n)

    df = pl.DataFrame({
        "tstamp": tstamps,
        "id": np.full(n, id_val),
        "sin": x,
        "cos": y,
        "cat": c
    })
    dfs.append(df)

df = pl.concat(dfs, how="vertical")

df.write_csv("sin.csv")
# df.write_csv("sin_short.csv")
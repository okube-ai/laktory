import numpy as np
from typing import Union
from dateutil import parser

from datetime import datetime
from datetime import timezone
from datetime import date

from planck import units


def unix_timestamp(
    dt: Union[str, int, float, datetime, date] = None,
    unit: str = "s",
    as_int: bool = False,
) -> float:
    """
    Convert a datetime object into a unix timestamp float. If `None` is provided,
    current UTC timestamp is returned.

    Parameters
    ----------
    dt:
        Datetime object

    Returns
    -------
    output:
        Unix timestamp

    Examples
    --------
    ```py
    from laktory.datetime import unix_timestamp
    from datetime import datetime

    ts = unix_timestamp("2020-01-01T01:00:00")
    print(ts)
    #> 1577840400.0

    ts = unix_timestamp(datetime(2020, 1, 1, 1, 0, 0))
    print(ts)
    #> 1577840400.0
    ```
    """
    if dt is None:
        dt = datetime.utcnow()
    elif type(dt) in [int, float, np.float64, np.int64]:
        dt = dt
    elif type(dt) == date:
        dt = datetime.combine(dt, datetime.min.time())
    elif isinstance(dt, str):
        dt = parser.parse(dt)
    elif isinstance(dt, np.datetime64):
        dt = dt.astype(datetime)
        if isinstance(dt, int):
            dt = dt * units["ns"]["s"]

    if isinstance(dt, datetime) or isinstance(dt, date):
        # if pd.isnull(dt):
        #     dt = np.nan
        # else:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.timestamp()

    # Set units
    dt *= units["s"][unit]

    # Set type
    if as_int:
        dt = int(dt)

    return dt


def utc_datetime(
    unixtime: Union[datetime, date, str, float, int] = None,
) -> datetime:
    """
    Convert a unix timestamp into a datetime object. If `None` is provided,
    current UTC datetime object is returned.

    Parameters
    ----------
    unixtime:
        Unix timestamp

    Returns
    -------
    output:
        Datetime object

    Examples
    --------
    ```py
    from laktory.datetime import utc_datetime

    dt = utc_datetime("2020-01-01T01:00:00")
    print(dt)
    #> 2020-01-01 01:00:00

    dt = utc_datetime(1577840400)
    print(dt)
    #> 2020-01-01 01:00:00
    ```
    """
    if not unixtime:
        dt = datetime.utcnow()
    elif isinstance(unixtime, datetime):
        dt = unixtime
    elif isinstance(unixtime, date):
        dt = datetime.combine(unixtime, datetime.min.time())
    elif isinstance(unixtime, str):
        dt = datetime.fromisoformat(unixtime)
    elif np.isnan(unixtime):
        dt = None
    else:
        dt = datetime.fromtimestamp(unixtime, tz=timezone.utc).replace(tzinfo=None)

    return dt

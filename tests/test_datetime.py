from datetime import datetime
from datetime import date
from planck import units
import numpy as np

from laktory import unix_timestamp
from laktory import utc_datetime


def test_unix_timestamp():
    utstamp = 1577840400  # Wednesday, January 1, 2020 1:00:00 AM
    utstamp0 = 1577836800  # Wednesday, January 1, 2020 0:00:00 AM
    dt = datetime(2020, 1, 1, 1, 0, 0)

    assert utc_datetime(utstamp) == dt
    assert unix_timestamp(utc_datetime(utstamp)) == utstamp
    assert unix_timestamp(date(2020, 1, 1)) == utstamp0
    assert utc_datetime(dt) == dt
    assert unix_timestamp(utstamp) == utstamp
    assert utc_datetime("2020-01-01T01:00:00") == dt
    assert unix_timestamp("2020-01-01T01:00:00") == utstamp

    # Null value
    assert utc_datetime(np.datetime64("nat")) is None

    # Timezone
    utstamp0 = unix_timestamp("2022-08-02 15:43:54.199855-02:00")
    utstamp1 = unix_timestamp("2022-08-02T15:43:54.199855+03:00")
    assert utstamp0 - utstamp1 == 5 * units["h"]["s"]


if __name__ == "__main__":
    test_unix_timestamp()

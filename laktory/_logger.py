import logging
import sys
from zoneinfo import ZoneInfo
from datetime import datetime

from ._settings import settings


# --------------------------------------------------------------------------- #
# Custom Formatter                                                            #
# --------------------------------------------------------------------------- #


class LaktoryFormatter(logging.Formatter):
    info_format = "%(asctime)s [laktory] %(message)s"
    default_format = "%(asctime)s [%(name)s] %(levelname)s | %(message)s"

    def __init__(self, fmt=default_format):
        super().__init__(fmt=fmt, datefmt="%Y-%m-%d %H:%M:%S")

    def formatTime(self, record, datefmt=None):
        # Set timestamp to UTC
        utc_time = datetime.fromtimestamp(record.created, tz=ZoneInfo("UTC"))
        return utc_time.strftime(datefmt or self.default_time_format)

    def format(self, record):
        # Use info-specific format, otherwise use default
        if record.levelno == logging.INFO:
            self._style._fmt = self.info_format
        else:
            self._style._fmt = self.default_format
        return super().format(record)


# --------------------------------------------------------------------------- #
# Get logger                                                                  #
# --------------------------------------------------------------------------- #

# Stream Handler
stream_handler = logging.StreamHandler(sys.stdout)
# format = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
# stream_handler.setFormatter(format)
stream_handler.setFormatter(LaktoryFormatter())


def get_logger(name, stream=True):
    # Change logger class
    # TODO: Place Holder if we need to customize logger class
    # logging.setLoggerClass(LaktoryLogger)

    # Get logger
    logger = logging.getLogger(name)

    # Set level
    logger.setLevel(settings.log_level)

    # Set options
    _options = {
        "stream": stream,
    }

    # Set states
    _initialized = hasattr(logger, "_initialized")
    _options_changed = False
    if _initialized:
        _options_changed = logger._options != _options

    # Update handlers
    if not _initialized or _options_changed:
        handlers = list(logger.handlers)
        for handler in handlers:
            logger.removeHandler(handler)

        if stream:
            logger.addHandler(stream_handler)

    # Reset logger class
    logging.setLoggerClass(logging.Logger)

    # Update state
    logger._initialized = True
    logger._options = _options

    return logger

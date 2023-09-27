import logging
import sys

from ._settings import settings

# --------------------------------------------------------------------------- #
# Get logger                                                                  #
# --------------------------------------------------------------------------- #

# Stream Handler
stream_handler = logging.StreamHandler(sys.stdout)
format = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
stream_handler.setFormatter(format)


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

from ._version import VERSION

__version__ = VERSION

# --------------------------------------------------------------------------- #
# Packages                                                                    #
# --------------------------------------------------------------------------- #

# Required to parse version before build?
try:
    from ._settings import settings
except ModuleNotFoundError:
    pass


# --------------------------------------------------------------------------- #
# Classes                                                                     #
# --------------------------------------------------------------------------- #

# Required to parse version before build?
try:
    from ._settings import Settings
except ModuleNotFoundError:
    pass

# --------------------------------------------------------------------------- #
# Objects                                                                     #
# --------------------------------------------------------------------------- #

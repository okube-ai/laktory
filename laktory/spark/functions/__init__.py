from functools import wraps

import pyspark.sql.functions as F

import laktory.spark.functions.logical as logical
import laktory.spark.functions.math as math
import laktory.spark.functions.string as string
import laktory.spark.functions.units as units

# For documentation
from laktory.spark.functions.logical import *  # noqa: F403
from laktory.spark.functions.math import *  # noqa: F403
from laktory.spark.functions.string import *  # noqa: F403
from laktory.spark.functions.units import *  # noqa: F403


class LaktoryFunctions:
    # ----------------------------------------------------------------------- #
    # Logical                                                                 #
    # ----------------------------------------------------------------------- #

    @staticmethod
    def compare(*args, **kwargs):
        return logical.compare(*args, **kwargs)

    # ----------------------------------------------------------------------- #
    # Math                                                                    #
    # ----------------------------------------------------------------------- #

    @staticmethod
    @wraps(math.roundp)
    def roundp(*args, **kwargs):
        return math.roundp(*args, **kwargs)

    # ----------------------------------------------------------------------- #
    # String                                                                  #
    # ----------------------------------------------------------------------- #

    @staticmethod
    @wraps(string.string_split)
    def string_split(*args, **kwargs):
        return string.string_split(*args, **kwargs)

    @staticmethod
    @wraps(string.uuid)
    def uuid(*args, **kwargs):
        return string.uuid(*args, **kwargs)

    # ----------------------------------------------------------------------- #
    # Units                                                                   #
    # ----------------------------------------------------------------------- #

    @staticmethod
    @wraps(units.convert_units)
    def convert_units(*args, **kwargs):
        return units.convert_units(*args, **kwargs)


F.laktory = LaktoryFunctions()

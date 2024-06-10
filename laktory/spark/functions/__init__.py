from functools import wraps
import pyspark.sql.functions as F

import laktory.spark.functions.logical
import laktory.spark.functions.math
import laktory.spark.functions.string
import laktory.spark.functions.units

# For documentation
from laktory.spark.functions.logical import *
from laktory.spark.functions.math import *
from laktory.spark.functions.string import *
from laktory.spark.functions.units import *


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

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
    @wraps(math.add)
    def add(*args, **kwargs):
        return math.add(*args, **kwargs)

    @staticmethod
    @wraps(math.sub)
    def sub(*args, **kwargs):
        return math.sub(*args, **kwargs)

    @staticmethod
    @wraps(math.mul)
    def mul(*args, **kwargs):
        return math.mul(*args, **kwargs)

    @staticmethod
    @wraps(math.div)
    def div(*args, **kwargs):
        return math.div(*args, **kwargs)

    @staticmethod
    @wraps(math.poly1)
    def poly1(*args, **kwargs):
        return math.poly1(*args, **kwargs)

    @staticmethod
    @wraps(math.poly2)
    def poly2(*args, **kwargs):
        return math.poly2(*args, **kwargs)

    @staticmethod
    @wraps(math.add)
    def scaled_power(*args, **kwargs):
        return math.scaled_power(*args, **kwargs)

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

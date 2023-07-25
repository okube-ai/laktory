from pyspark.sql.dataframe import DataFrame

from laktory.readers.basereader import BaseReader


class AWSKinesisReader(BaseReader):

    def read(self, spark) -> DataFrame:
        raise NotImplementedError()

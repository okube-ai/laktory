from pyspark.sql.dataframe import DataFrame

from laktory.readers.basereader import BaseReader


class KafkaReader(BaseReader):

    def read(self, spark) -> DataFrame:
        raise NotImplementedError()

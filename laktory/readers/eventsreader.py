from pyspark.sql.dataframe import DataFrame

from laktory.readers.basereader import BaseReader


class EventsReader(BaseReader):
    load_path: str = ""

    def read(self, spark) -> DataFrame:
        return (
            spark
            .read
            .option("multiLine", self.multiline)
            .option("mergeSchema", True)
            .option("recursiveFileLookup", True)
            .format(self.fmt)
            .load(self.load_path)
        )

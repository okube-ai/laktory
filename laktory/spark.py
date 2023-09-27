spark_installed = False
try:
    from pyspark.sql import DataFrame
    from pyspark.sql.utils import AnalysisException
    spark_installed = True

except ModuleNotFoundError:
    class DataFrame:
        pass



def df_has_column(sdf, col):
    try:
        sdf.select(col)
        return True
    except AnalysisException:
        return False



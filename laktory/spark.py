from pyspark.sql.utils import AnalysisException


def df_has_column(sdf, col):
    try:
        sdf.select(col)
        return True
    except AnalysisException:
        return False
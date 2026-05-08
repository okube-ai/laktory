from laktory._settings import settings

if settings.register_nw_extensions:
    import laktory.narwhals_ext.dataframe
    import laktory.narwhals_ext.expr
    import laktory.narwhals_ext.functions

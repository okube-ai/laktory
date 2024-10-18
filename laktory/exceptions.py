class MissingColumnError(Exception):
    """"""

    def __init__(self, message="Column is missing", column_name=None):
        if column_name:
            message = f"Column {column_name} is missing"
        super().__init__(message)


class MissingColumnsError(Exception):
    def __init__(self, message="All columns are missing", column_names=None):
        if column_names:
            message = f"Column {column_names} are missing"
        super().__init__(message)


class DataQualityCheckFailedError(Exception):
    def __init__(self, check, node=None):

        message = f"Expectation '{check.expectation.name}' failed"
        if node:
            message += f" on node '{node.name}'"
        message += f" | {check.log_msg}"
        super().__init__(message)

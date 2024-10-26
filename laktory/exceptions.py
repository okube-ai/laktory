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
    def __init__(self, expectation, node=None):

        message = f"Expectation '{expectation.name}' failed"
        if node:
            message += f" on node '{node.name}'"
        message += f" | {expectation.log_msg}"
        super().__init__(message)


class DataQualityExpectationsNotSupported(Exception):
    def __init__(self, expectation, node=None):
        message = (
            f"Expectation '{expectation.name}' not supported for streaming DataFrame"
        )
        if node:
            message += f" on node '{node.name}'"
        message += (
            ". Only ROW type expectations with 0 absolute tolerances are allowed."
        )
        super().__init__(message)

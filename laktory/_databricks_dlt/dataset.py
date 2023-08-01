from enum import Enum


class Dataset:
    # An immutable unique id for a dataset. We need this since a dataset can be created before its
    # name is finalized. For example, this can happen when an expectation decorator is chained with
    # create_table.
    uid = 0

    def __init__(self):
        self.uid = Dataset.uid
        Dataset.uid += 1
        self.name = None
        self.func = None
        self.expectations = []
        # Whether the name for the dataset can no longer be changed. This happens when a create_view
        # create_table decorator has been called and evaluated.
        self.name_finalized = False
        self.builder = None


class ViolationAction(Enum):
    ALLOW = 1
    DROP = 2
    FAIL = 3


class Expectation:
    def __init__(self, name, inv, action):
        self.name = name
        self.inv = inv
        self.action = action

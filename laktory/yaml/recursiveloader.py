from pathlib import Path

import yaml

from laktory._parsers import _resolve_value

MERGE_KEY = "__merge_here"
VARIABLES_KEY = "variables"


# Register the constructors
class RecursiveLoader(yaml.SafeLoader):
    def __init__(self, stream, parent_loader=None):
        self.dirpath = Path("./")
        stream = self.preprocess_stream(stream)

        self.variables = []
        if parent_loader:
            self.variables = parent_loader.variables
        super().__init__(stream)

    def preprocess_stream(self, stream):
        """Reformat content to be YAML safe"""

        if hasattr(stream, "name"):
            self.dirpath = Path(stream.name).parent

        _lines = []
        for line in stream.readlines():
            if "${include." in line:
                raise ValueError(
                    "The `${include.}` syntax has been deprecated in laktory 0.6.0. Please use `!use`, `!update` and `!extend` tags instead."
                )
            _lines += [line.replace("<<:", MERGE_KEY + ":")]

        return "\n".join(_lines)

    @classmethod
    def load(cls, stream, parent_loader: "RecursiveLoader" = None):
        """
        Load yaml file with support for reference to external yaml and sql files using
        `!use`, `!extend` and `!update` tags.
        Path to external files can be defined using model or environment variables.

        Custom Tags
        -----------
        !use {filepath}:
            Directly inject the content of the file at `filepath`

        - !extend {filepath}:
            Extend the current list with the elements found in the file at `filepath`.
            Similar to python list.extend method.

        <<: !update {filepath}:
            Merge the current dictionary with the content of the dictionary defined at
            `filepath`. Similar to python dict.update method.

        Parameters
        ----------
        stream:
            file object structured as a yaml file
        parent_loader:
            Parent loader if file loader from another loader.

        Returns
        -------
        :
            Dict or list

        Examples
        --------
        ```yaml
        businesses:
          apple:
            symbol: aapl
            address: !use addresses.yaml
            <<: !update common.yaml
            emails:
              - jane.doe@apple.com
              - extend! emails.yaml
          amazon:
            symbol: amzn
            address: !use addresses.yaml
            <<: update! common.yaml
            emails:
              - john.doe@amazon.com
              - extend! emails.yaml
        ```
        """
        loader = cls(stream, parent_loader)
        try:
            return loader.get_single_data()
        finally:
            loader.dispose()

    @staticmethod
    def get_path(loader, node):
        filepath = Path(loader.construct_scalar(node))

        # set absolute path
        if not Path(filepath).is_absolute():
            filepath = loader.dirpath / filepath
        filepath = str(filepath)

        # resolve variables
        if "${vars." in filepath:
            variables = {}
            for _vars in loader.variables:
                variables.update(_vars)
            filepath = _resolve_value(str(filepath), variables)

            if "${vars." in filepath:
                raise ValueError(f"Some variables in {filepath} could not be resolved.")

        return filepath

    @staticmethod
    def inject_constructor(loader, node):
        """Inject content of another YAML file."""

        filepath = loader.get_path(loader, node)

        if filepath.endswith(".sql"):
            with open(filepath, "r", encoding="utf-8") as _fp:
                data = _fp.read()
            return data

        with open(filepath, "r") as f:
            return RecursiveLoader.load(f, parent_loader=loader)

    @staticmethod
    def merge_constructor(loader, node):
        """Merge content of another YAML file into the current dictionary."""

        filepath = loader.get_path(loader, node)
        with open(filepath, "r") as f:
            merge_data = RecursiveLoader.load(f, parent_loader=loader)

        if not isinstance(merge_data, dict):
            raise TypeError(
                f"Expected a dictionary in {filepath}, but got {type(merge_data).__name__}"
            )
        return merge_data

    @staticmethod
    def append_constructor(loader, node):
        """Append content of another YAML file to the current list."""

        filepath = loader.get_path(loader, node)
        with open(filepath, "r") as f:
            append_data = RecursiveLoader.load(f, parent_loader=loader)
        if not isinstance(append_data, list):
            raise TypeError(
                f"Expected a list in {filepath}, but got {type(append_data).__name__}"
            )
        return append_data

    # Custom mapping constructor to handle merging
    @staticmethod
    def custom_mapping_constructor(loader, node):
        """Custom handling for mappings to support !merge."""

        # read variables
        _vars = {}
        for key_node, value_node in node.value:
            key = loader.construct_object(key_node)
            if key == VARIABLES_KEY:
                _vars = loader.construct_object(value_node)
        loader.variables += [_vars]

        # read include and merge
        mapping = {}
        for key_node, value_node in node.value:
            key = loader.construct_object(key_node)
            value = loader.construct_object(value_node)
            if key == MERGE_KEY and isinstance(value, dict):
                # Merge the dictionary directly into the parent mapping
                mapping.update(value)
            else:
                mapping[key] = value

        # remove variables
        del loader.variables[-1]

        return mapping

    # Custom sequence constructor to handle appending
    @staticmethod
    def custom_sequence_constructor(loader, node):
        """Custom handling for sequences to support !append."""

        seq = []
        for child in node.value:
            if child.tag == "!extend":
                append_data = loader.construct_object(child)
                seq.extend(append_data)  # Flatten the appended list
            else:
                seq.append(loader.construct_object(child))
        return seq


RecursiveLoader.add_constructor("!use", RecursiveLoader.inject_constructor)
RecursiveLoader.add_constructor("!update", RecursiveLoader.merge_constructor)
RecursiveLoader.add_constructor("!extend", RecursiveLoader.append_constructor)
RecursiveLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_SEQUENCE_TAG,
    RecursiveLoader.custom_sequence_constructor,
)
RecursiveLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    RecursiveLoader.custom_mapping_constructor,
)

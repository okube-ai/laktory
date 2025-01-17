from pathlib import Path

import yaml

MERGE_KEY = "__merge_here"


# Register the constructors
class RecursiveLoader(yaml.SafeLoader):
    def __init__(self, stream):
        self.dirpath = Path("./")
        stream = self.preprocess_stream(stream)
        super().__init__(stream)

    def preprocess_stream(self, stream):
        """Reformat content to be YAML safe"""

        if hasattr(stream, "name"):
            self.dirpath = Path(stream.name).parent

        _lines = []
        for line in stream.readlines():
            _lines += [line.replace("<<:", MERGE_KEY + ":")]
        return "\n".join(_lines)

    @classmethod
    def load(cls, fp):
        return yaml.load(fp, Loader=cls)

    @staticmethod
    def get_path(loader, node):
        filepath = Path(loader.construct_scalar(node))
        if not Path(filepath).is_absolute():
            filepath = loader.dirpath / filepath
        return str(filepath)

    @staticmethod
    def inject_constructor(loader, node):
        """Inject content of another YAML file."""

        filepath = loader.get_path(loader, node)

        if "${vars." in str(filepath):
            return f"!inject: {str(filepath)}"

        with open(filepath, "r") as f:
            return yaml.load(f, Loader=RecursiveLoader)

    @staticmethod
    def merge_constructor(loader, node):
        """Merge content of another YAML file into the current dictionary."""

        filepath = loader.get_path(loader, node)

        if "${vars." in filepath:
            return [f"{MERGE_KEY}: {filepath}"]

        with open(filepath, "r") as f:
            merge_data = yaml.load(f, Loader=RecursiveLoader)
        if not isinstance(merge_data, dict):
            raise TypeError(
                f"Expected a dictionary in {filepath}, but got {type(merge_data).__name__}"
            )
        return merge_data

    @staticmethod
    def append_constructor(loader, node):
        """Append content of another YAML file to the current list."""

        filepath = loader.get_path(loader, node)

        if "${vars." in filepath:
            return [f"!append: {filepath}"]

        with open(filepath, "r") as f:
            append_data = yaml.load(f, Loader=RecursiveLoader)
        if not isinstance(append_data, list):
            raise TypeError(
                f"Expected a list in {filepath}, but got {type(append_data).__name__}"
            )
        return append_data

    # Custom mapping constructor to handle merging
    @staticmethod
    def custom_mapping_constructor(loader, node):
        """Custom handling for mappings to support !merge."""
        mapping = {}
        for key_node, value_node in node.value:
            key = loader.construct_object(key_node)
            value = loader.construct_object(value_node)

            if isinstance(value, dict) and key == MERGE_KEY:
                # Merge the dictionary directly into the parent mapping
                mapping.update(value)
            else:
                mapping[key] = value
        return mapping

    # Custom sequence constructor to handle appending
    @staticmethod
    def custom_sequence_constructor(loader, node):
        """Custom handling for sequences to support !append."""
        seq = []
        for child in node.value:
            if child.tag == "!append":
                append_data = loader.construct_object(child)
                seq.extend(append_data)  # Flatten the appended list
            else:
                seq.append(loader.construct_object(child))
        return seq


RecursiveLoader.add_constructor("!inject", RecursiveLoader.inject_constructor)
RecursiveLoader.add_constructor("!merge", RecursiveLoader.merge_constructor)
RecursiveLoader.add_constructor("!append", RecursiveLoader.append_constructor)
RecursiveLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_SEQUENCE_TAG,
    RecursiveLoader.custom_sequence_constructor,
)
RecursiveLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    RecursiveLoader.custom_mapping_constructor,
)

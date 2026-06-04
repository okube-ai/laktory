import re
from pathlib import Path

import yaml

from laktory._parsers import _resolve_value

MERGE_KEY = "__merge_here"
VARIABLES_KEY = "variables"


# Register the constructors
class RecursiveLoader(yaml.SafeLoader):
    def __init__(self, stream, parent_loader=None, vars=None):
        self.dirpath = Path("./")
        self._loading_paths: set[str] = (
            set(parent_loader._loading_paths) if parent_loader else set()
        )
        stream = self.preprocess_stream(stream)

        self.variables = []
        if vars:
            self.variables += [vars]
        if parent_loader:
            self.variables += parent_loader.variables
        super().__init__(stream)

    def preprocess_stream(self, stream):
        """Reformat content to be YAML safe"""

        if hasattr(stream, "name"):
            self.dirpath = Path(stream.name).parent
            self._loading_paths.add(str(Path(stream.name).resolve()))

        _lines = []
        for line in stream.readlines():
            if "${include." in line:
                raise ValueError(
                    "The `${include.}` syntax has been deprecated in laktory 0.6.0. Please use `!use`, `!update` and `!extend` tags instead."
                )
            # Only replace <<: at the start of line content (after optional
            # whitespace) - the only valid YAML position for a merge key.
            line = re.sub(r"^(\s*)<<:", r"\g<1>" + MERGE_KEY + ":", line)
            # Unquoted {nodes.X} / {sources.X} are parsed by PyYAML as flow
            # mappings instead of strings. Auto-quote them so they reach
            # DataFrameMethodArg as the intended string reference.
            line = re.sub(
                r'(?<!["\'])\{((?:nodes|sources)\.[^}]+)\}(?!["\'])',
                r'"{\1}"',
                line,
            )
            _lines += [line]

        return "\n".join(_lines)

    @classmethod
    def load(cls, stream, parent_loader: "RecursiveLoader" = None, vars=None):
        """
        Load yaml file with support for reference to external yaml and sql files using
        `!use`, `!extend` and `!update` tags.
        Path to external files can be defined using model or environment variables.

        Custom Tags
        -----------
        !use {filepath}:
            Directly inject the content of the file at `filepath`. A directory can also
            be provided. In this case, each yaml file found in the directory will be
            loaded as an element of a list.

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
        vars:
            Dict of variables available when parsing filepaths references in yaml files
            i.e. `!use catalog_${vars.env}.yaml`

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
        loader = cls(stream, parent_loader, vars=vars)
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
            filepath = _resolve_value(str(filepath), variables, {})

            if "${vars." in filepath:
                raise ValueError(f"Some variables in {filepath} could not be resolved.")

        return filepath

    @staticmethod
    def inject_constructor(loader, node):
        """Inject content of another YAML file."""

        filepath = Path(loader.get_path(loader, node))

        if filepath.as_posix().endswith(".sql"):
            try:
                with filepath.open("r", encoding="utf-8") as _fp:
                    data = _fp.read()
            except FileNotFoundError:
                raise FileNotFoundError(
                    f"!use target not found: '{filepath}' (referenced from '{loader.dirpath}')"
                )
            return data

        if filepath.is_dir():
            objs = []
            for _filepath in sorted(
                set(filepath.rglob("*.yaml")) | set(filepath.rglob("*.yml"))
            ):
                abs_path = str(_filepath.resolve())
                if abs_path in loader._loading_paths:
                    raise ValueError(
                        f"Circular !use reference: '{_filepath}' is already being loaded"
                    )
                with _filepath.open("r") as f:
                    objs += [RecursiveLoader.load(f, parent_loader=loader)]
            return objs

        else:
            abs_path = str(filepath.resolve())
            if abs_path in loader._loading_paths:
                raise ValueError(
                    f"Circular !use reference: '{filepath}' is already being loaded"
                )
            try:
                with filepath.open("r") as f:
                    return RecursiveLoader.load(f, parent_loader=loader)
            except FileNotFoundError:
                raise FileNotFoundError(
                    f"!use target not found: '{filepath}' (referenced from '{loader.dirpath}')"
                )

    @staticmethod
    def merge_constructor(loader, node):
        """Merge content of another YAML file into the current dictionary."""

        filepath = loader.get_path(loader, node)
        abs_path = str(Path(filepath).resolve())
        if abs_path in loader._loading_paths:
            raise ValueError(
                f"Circular !update reference: '{filepath}' is already being loaded"
            )
        try:
            with open(filepath, "r") as f:
                merge_data = RecursiveLoader.load(f, parent_loader=loader)
        except FileNotFoundError:
            raise FileNotFoundError(
                f"!update target not found: '{filepath}' (referenced from '{loader.dirpath}')"
            )

        if not isinstance(merge_data, dict):
            raise TypeError(
                f"Expected a dictionary in {filepath}, but got {type(merge_data).__name__}"
            )
        return merge_data

    @staticmethod
    def append_constructor(loader, node):
        """Append content of another YAML file to the current list."""

        filepath = loader.get_path(loader, node)
        abs_path = str(Path(filepath).resolve())
        if abs_path in loader._loading_paths:
            raise ValueError(
                f"Circular !extend reference: '{filepath}' is already being loaded"
            )
        try:
            with open(filepath, "r") as f:
                append_data = RecursiveLoader.load(f, parent_loader=loader)
        except FileNotFoundError:
            raise FileNotFoundError(
                f"!extend target not found: '{filepath}' (referenced from '{loader.dirpath}')"
            )
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
        try:
            mapping = {}
            for key_node, value_node in node.value:
                key = loader.construct_object(key_node)
                value = loader.construct_object(value_node)
                if key == MERGE_KEY and isinstance(value, dict):
                    # Merge the dictionary directly into the parent mapping
                    mapping.update(value)
                else:
                    mapping[key] = value
        finally:
            # Always restore the variables stack, even if construction fails
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

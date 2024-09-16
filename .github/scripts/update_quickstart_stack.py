import os
import re
import pathlib
import argparse


def main(branch_name: str, stack_root: str):

    for dirpath, dirnames, filenames in os.walk(stack_root):
        dirpath = pathlib.Path(dirpath)

        for filename in filenames:
            filepath = dirpath / filename

            if filepath.suffix not in [".yaml", ".yml", ".py"]:
                continue

            with open(filepath, "r") as fp:
                data = fp.read()

            pattern = r"pip install ['\"]?laktory([=<>!~]*[^\s'\"]*)?['\"]?"

            matches = re.findall(pattern, data)
            if matches:
                print(f"Updating {filepath}")
                data = re.sub(
                    pattern,
                    f"pip install git+https://github.com/okube-ai/laktory.git@{branch_name}",
                    data,
                )

                with open(filepath, "w") as fp:
                    fp.write(data)


if __name__ == "__main__":
    # Parse arguments
    parser = argparse.ArgumentParser(
        description="Update notebooks to install specific version of laktory"
    )
    parser.add_argument("branch_name", type=str, help="Laktory branch name")
    parser.add_argument(
        "--stack_root",
        type=str,
        help="Stack directory",
        default="./",
    )
    args = parser.parse_args()

    # Execute
    main(args.branch_name, args.stack_root)

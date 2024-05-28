import os
import argparse


def main(branch_name: str, notebooks_dir: str):

    for filename in os.listdir(notebooks_dir):
        filepath = os.path.join(notebooks_dir, filename)

        print(f"Updating {filepath} with laktory from branch {branch_name}")

        with open(filepath, "r") as fp:
            data = fp.read()

        if "pip install laktory" not in data:
            continue

        data = data.replace(
            "pip install laktory",
            f"pip install git+https://github.com/okube-ai/laktory.git@{branch_name}",
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
        "--notebooks_path",
        type=str,
        help="Path of the notebooks directory",
        default="./notebooks/dlt",
    )
    args = parser.parse_args()

    # Execute
    main(args.branch_name, args.notebooks_path)

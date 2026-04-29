import argparse
import os
import pathlib
import re


def main(branch_name: str, stack_root: str):
    for dirpath, dirnames, filenames in os.walk(stack_root):
        dirpath = pathlib.Path(dirpath)

        # Update Laktory Version
        for filename in filenames:
            filepath = dirpath / filename

            if filepath.suffix not in [".yaml", ".yml", ".py"]:
                continue

            with open(filepath, "r") as fp:
                data = fp.read()

            pattern = r"['\"]?laktory==([=<>!~]*[^\s'\"]*)?['\"]?"

            matches = re.findall(pattern, data)
            if matches:
                print(f"Updating {filepath}")
                data = re.sub(
                    pattern,
                    f"git+https://github.com/okube-ai/laktory.git@{branch_name}",
                    data,
                )

                with open(filepath, "w") as fp:
                    fp.write(data)

        # Update terraform backend
        filepath = dirpath / "stack.yaml"
        newlines = [
            "",
            "terraform:",
            "   backend:",
            "      azurerm:",
            "          resource_group_name: o3-rg-laktory-dev",
            "          storage_account_name: o3stglaktorydev",
            "          container_name: terraform",
            f'          key: "states/{dirpath.name}/terraform.tfstate"',
            "          use_azuread_auth: true",
            "          client_id: ${vars.AZURE_CLIENT_ID}",
            "          client_secret: ${vars.AZURE_CLIENT_SECRET}",
            "          tenant_id: ${vars.AZURE_TENANT_ID}",
            "          subscription_id: c8b10a15-5bb2-4c3f-988a-8ec6e60614bb",
        ]

        with open(filepath, "r") as fp:
            lines = fp.readlines()

        lines = lines + newlines

        with open(filepath, "w") as fp:
            fp.writelines(lines)


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

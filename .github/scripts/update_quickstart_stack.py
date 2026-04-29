import argparse
import os
import pathlib
import re


def main(branch_name: str, template: str, stack_root: str):
    stack_root = pathlib.Path(stack_root)

    for dirpath, dirnames, filenames in os.walk(stack_root.as_posix()):
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
        if template in [
            "unity-catalog",
            "workspace",
            "workflows",
        ]:
            newlines = [
                "\n",
                "terraform:\n",
                "   backend:\n",
                "      azurerm:\n",
                "          resource_group_name: o3-rg-laktory-dev\n",
                "          storage_account_name: o3stglaktorydev\n",
                "          container_name: terraform\n",
                f'          key: "states/{template}/terraform.tfstate"\n',
                "          use_azuread_auth: true\n",
                "          client_id: ${vars.AZURE_CLIENT_ID}\n",
                "          client_secret: ${vars.AZURE_CLIENT_SECRET}\n",
                "          tenant_id: ${vars.AZURE_TENANT_ID}\n",
                "          subscription_id: c8b10a15-5bb2-4c3f-988a-8ec6e60614bb\n",
            ]

            filepath = stack_root / "stack.yaml"
            with open(filepath, "r") as fp:
                lines = fp.readlines()

            with open(filepath, "w") as fp:
                fp.writelines(lines + newlines)

            for line in lines + newlines:
                print(line)

            print()
            for k in [
                "AZURE_CLIENT_ID",
                "AZURE_CLIENT_SECRET",
                "AZURE_TENANT_ID",
            ]:
                value = os.getenv(k)
                print(k, "___".join(value))


if __name__ == "__main__":
    # Parse arguments
    parser = argparse.ArgumentParser(
        description="Update notebooks to install specific version of laktory"
    )
    parser.add_argument("branch_name", type=str, help="Laktory branch name")
    parser.add_argument("template", type=str, help="Quickstart Template")
    parser.add_argument(
        "--stack_root",
        type=str,
        help="Stack directory",
        default="./",
    )
    args = parser.parse_args()

    # Execute
    main(args.branch_name, args.template, args.stack_root)

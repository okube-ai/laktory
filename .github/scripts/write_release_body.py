import os

script_root = os.path.dirname(__file__)
package_name = "laktory"
package_root = f"./{package_name}/"


def main():
    # Filepaths
    version_filepath = os.path.join(package_root, "_version.py")
    changelog_filepath = os.path.join("./", "CHANGELOG.md")
    body_filepath = os.path.join(script_root, "release_body.md")

    # Read version file
    with open(version_filepath) as fp:
        release_version = fp.read().split("=")[-1].strip().replace('"', "")
    print(f"Get changes for {package_name} {release_version}")

    # Read changelog
    with open(changelog_filepath, "r") as fp:
        content = fp.read()
    blocks = content.split("## [")

    # Select latest changes
    changes = blocks[1]
    changes = "\n".join(changes.split("\n")[1:]).strip()
    changes = changes.replace("###", "####")

    # Add header
    changes = f"### What’s new\n{changes}"

    # Add compare to previous release
    previous_version = blocks[2].split("]")[0]
    changes += f"\n\nFull [Changelog](https://github.com/okube-ai/{package_name}/compare/v{previous_version}...v{release_version}/)"

    print("Release Content")
    print("---------------")
    print(changes)
    print("--------------")

    # Write body
    print(f"Writing body {body_filepath}")
    with open(body_filepath, "w") as fp:
        fp.write(changes)

    if os.getenv("GITHUB_OUTPUT") is None:
        os.remove(body_filepath)


if __name__ == "__main__":
    main()

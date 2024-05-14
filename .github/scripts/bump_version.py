import os
import logging
from datetime import datetime
from packaging import version

script_root = os.path.dirname(__file__)
package_name = "laktory"
laktory_root = f"./{package_name}/"


def main():
    # Filepaths
    version_filepath = os.path.join(laktory_root, "_version.py")
    local_env_filepath = os.path.join(script_root, "git.env")
    git_env_filepath = os.getenv("GITHUB_OUTPUT", local_env_filepath)
    changelog_filepath = os.path.join("./", "CHANGELOG.md")

    # Read version file
    with open(version_filepath) as fp:
        v0 = fp.read().split("=")[-1].strip().replace('"', "")
        v0 = version.parse(v0)
        v1 = version.Version(f"{v0.major}.{v0.minor}.{v0.micro + 1}")
    print(f"Bumping laktory to {v1}")

    # Update version file
    print(f"Updating _version.py")
    with open(version_filepath, "w") as fp:
        fp.write(f'VERSION = "{v1}"\n')

    # Set version as git action variable
    print(f"Setting git env var version {git_env_filepath}")
    with open(git_env_filepath, "a") as fp:
        fp.write(f"version={v1}")

    # Update CHANGELOG
    update_changelog(changelog_filepath, v0, v1)

    # Cleanup
    if git_env_filepath == local_env_filepath and os.path.exists(local_env_filepath):
        os.remove(local_env_filepath)


def update_changelog(changelog_filepath, v0, v1):
    # Update CHANGELOG
    print(f"Updating changelog")
    with open(changelog_filepath, "r") as fp:
        content = fp.read()

    # Set version line
    version_line = f"## [{v0}] - Unreleased"

    # Validate
    if version_line not in content:
        msg = f"CHANGELOG  does not include version {v0}"
        print("ValueError: " + msg)
        raise ValueError(msg)

    # Update current version release date
    today = datetime.utcnow()
    new_version_line = f"## [{v0}] - {today.strftime('%Y-%m-%d')}"
    content = content.replace(version_line, new_version_line)

    # Add placeholder for next version
    next_version_lines = [
        "# Release History",
        "",
        f"## [{v1}] - Unreleased",
        "### Added",
        "* n/a",
        "### Fixed",
        "* n/a",
        "### Updated",
        "* n/a",
        "### Breaking changes",
        "* n/a",
        "",
    ]
    content = content.replace("# Release History\n", "\n".join(next_version_lines))

    # Write
    with open(changelog_filepath, "w") as fp:
        fp.write(content)


if __name__ == "__main__":
    main()

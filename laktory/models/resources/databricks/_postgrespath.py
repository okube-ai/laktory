import re


def postgres_path_key(parent: str | None, child_id: str | None = None) -> str:
    """
    Resource key from a Lakebase Autoscaling resource path, keeping the ids
    only: `projects/app/branches/production` + `primary` ->
    `app-production-primary`.
    """
    path = re.sub(r"(^|/)(projects|branches|databases)/", r"\1", parent or "")
    key = path.replace("/", "-")
    if child_id:
        key = f"{key}-{child_id}"
    return key

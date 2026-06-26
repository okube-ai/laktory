from __future__ import annotations


def validate_yaml(yaml_content: str) -> dict:
    try:
        import yaml as _yaml
    except ImportError:
        return {"valid": False, "errors": ["PyYAML is required for YAML validation"]}

    try:
        data = _yaml.safe_load(yaml_content)
    except _yaml.YAMLError as exc:
        return {"valid": False, "errors": [f"YAML parse error: {exc}"]}

    if not isinstance(data, dict):
        return {"valid": False, "errors": ["YAML content must be a mapping"]}

    model_cls = _detect_model(data)
    if model_cls is None:
        return {
            "valid": False,
            "errors": [
                "Cannot detect model type. Add a 'name' field or recognizable top-level keys."
            ],
        }

    from pydantic import ValidationError

    try:
        model_cls.model_validate(data)
    except ValidationError as exc:
        errors = [
            f"{'.'.join(str(l) for l in e['loc'])}: {e['msg']}" for e in exc.errors()
        ]
        return {"valid": False, "errors": errors}

    return {"valid": True}


def _detect_model(data: dict):
    from laktory.models.pipeline.pipeline import Pipeline
    from laktory.models.stacks.stack import Stack

    if "nodes" in data or "orchestrator" in data:
        return Pipeline
    if "resources" in data or "backend" in data or "environments" in data:
        return Stack
    if "name" in data:
        return Pipeline
    return None

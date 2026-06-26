from __future__ import annotations


def validate_yaml(yaml_content: str, model_name: str | None = None) -> dict:
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

    if model_name is not None:
        from laktory.mcp._model_docs import _MODEL_REGISTRY
        from laktory.mcp._model_docs import _load_class

        ref = next(
            (
                r
                for refs in _MODEL_REGISTRY.values()
                for r in refs
                if r.rsplit(":", 1)[1].lower() == model_name.lower()
            ),
            None,
        )
        if ref is None:
            available = sorted(
                r.rsplit(":", 1)[1] for refs in _MODEL_REGISTRY.values() for r in refs
            )
            return {
                "valid": False,
                "errors": [
                    f'Model "{model_name}" not found. '
                    f"Available models: {', '.join(available)}"
                ],
            }
        model_cls = _load_class(ref)
    else:
        model_cls = _detect_model(data)
        if model_cls is None:
            return {
                "valid": False,
                "errors": [
                    "Cannot auto-detect model type. "
                    "Pass model_name (e.g. model_name='Cluster') to validate any Laktory model."
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

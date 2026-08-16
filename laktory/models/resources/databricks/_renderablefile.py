from pathlib import Path

from pydantic import Field

from laktory._settings import settings


class RenderableFileMixin:
    """
    Shared support for resources with a field pointing at a local file whose
    content is uploaded verbatim by Terraform (e.g. `WorkspaceFile.source`,
    `Dashboard.file_path`). Consuming classes opt a given resource into
    variable rendering via `render_vars`, and implement their own
    `xxx_`/computed-`xxx` override (see `WorkspaceFile.source` for the
    pattern) that calls `_staged_path`/`_render_to_staged_path` from this
    mixin.
    """

    render_vars: bool = Field(
        False,
        description=(
            "If `True`, resolve `${vars.x}` / `${{ expr }}` placeholders in "
            "this file's content before upload. Resolved content is staged "
            "under `settings.build_root`; the staged copy is (re)written "
            "whenever `Stack.build()` runs (which `preview`, `deploy`, "
            "`destroy` and `build` all trigger)."
        ),
    )

    def _staged_path(self, subdir: str, key: str) -> str:
        """Deterministic build_root path for the rendered copy of this file."""
        return str(Path(settings.build_root) / subdir / key.lstrip("/"))

    def _render_to_staged_path(self, original: str, staged: str, vars: dict = None):
        """Read `original`, resolve variables, and write the result to `staged`."""
        original_p = Path(original)
        if not original_p.exists():
            raise FileNotFoundError(f"Rendered file source not found: '{original_p}'")

        resolved = self.resolve_string(
            original_p.read_text(encoding="utf-8-sig"), vars=vars
        )

        staged_p = Path(staged)
        staged_p.parent.mkdir(parents=True, exist_ok=True)
        staged_p.write_text(resolved, encoding="utf-8")

    @property
    def _rendered_field_value(self) -> str | None:
        """
        Current value of the field that gets redirected to a staged path
        when `render_vars` is `True` (e.g. `source`, `file_path`).
        Subclasses whose field isn't named `source` (e.g. `Dashboard`)
        override this.
        """
        return getattr(self, "source", None)

    def check_staged(self):
        """
        Raise a clear error if this resource is configured for variable
        rendering but its staged copy hasn't been written yet (i.e.
        `.build()` hasn't run). Guards against programmatic use that
        bypasses `Stack.build()` (the normal CLI flow always calls it
        before dumping resources).
        """
        if not self.render_vars:
            return
        staged = self._rendered_field_value
        if staged and not Path(staged).exists():
            raise RuntimeError(
                f"'{staged}' has not been rendered yet. Run `laktory build` "
                "(or `preview`/`deploy`, which trigger it automatically) "
                "before this resource is used."
            )

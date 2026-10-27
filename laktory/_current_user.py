import re
import uuid

_UNDERSCORES = re.compile(r"_+")


def _is_service_principal_name(user_name: str) -> bool:
    """Same heuristic as the Databricks CLI: a user name that parses as a
    UUID is a service principal application id."""
    try:
        uuid.UUID(user_name)
    except ValueError:
        return False
    return True


class CurrentUser:
    """Live Databricks identity, exposed as the `${current_user.x}` template
    namespace (see `docs/concepts/variables.md`).

    `None` until a `Stack` build/preview/deploy/validate finds a
    `${current_user.x}` reference somewhere in the stack and resolves it via
    a live Databricks SDK call (see `Stack._resolve_workspace_root` in
    `laktory/models/stacks/stack.py`) - never auto-injected or eagerly
    fetched, to avoid a surprise network call for stacks that don't
    reference it.
    """

    user_name: str | None = None
    display_name: str | None = None

    @property
    def short_name(self) -> str | None:
        """Short form of the user name, same algorithm as Declarative
        Automation Bundles' `${workspace.current_user.short_name}`
        (`GetShortUserName` in the Databricks CLI): the part before `@`,
        lowercased, every character other than a (unicode) letter or digit
        replaced by `_`, repeated `_` collapsed and leading/trailing `_`
        removed (`Jane.Doe-Smith@okube.ai` -> `jane_doe_smith`). For a
        service principal, its display name is used when set.
        """
        if self.user_name is None:
            return None
        name = self.user_name
        if _is_service_principal_name(name) and self.display_name:
            name = self.display_name
        local = name.split("@", 1)[0].lower()
        # `isdecimal()` matches Go's `unicode.IsDigit` (Nd category);
        # `isdigit()` would also keep superscripts
        s = "".join(c if c.isalpha() or c.isdecimal() else "_" for c in local)
        return _UNDERSCORES.sub("_", s).strip("_")


current_user = CurrentUser()

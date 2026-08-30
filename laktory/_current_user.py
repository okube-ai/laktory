class CurrentUser:
    """Live Databricks identity, exposed as the `${current_user.x}` template
    namespace (see `docs/concepts/variables.md`).

    `None` until a `Stack` build/preview/deploy/validate finds a
    `${current_user.x}` reference somewhere in the stack and resolves it via
    a live Databricks SDK call (see `Stack._resolve_user_root` in
    `laktory/models/stacks/stack.py`) - never auto-injected or eagerly
    fetched, to avoid a surprise network call for stacks that don't
    reference it.
    """

    user_name: str | None = None


current_user = CurrentUser()

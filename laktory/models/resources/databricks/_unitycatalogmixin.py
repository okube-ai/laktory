class UnityCatalogMixin:
    """
    Shared hierarchy properties for Unity Catalog resources (Schema, Table, Volume).
    Consuming classes must have catalog_name and/or schema_name fields declared
    in their Pydantic base.
    """

    @property
    def parent_full_name(self) -> str | None:
        """Parent namespace in the UC three-level hierarchy"""
        parts = [
            p
            for p in [
                getattr(self, "catalog_name", None),
                getattr(self, "schema_name", None),
            ]
            if p
        ]
        return ".".join(parts) if parts else None

    @property
    def full_name(self) -> str:
        """Full UC identifier including parent namespace"""
        parent = self.parent_full_name
        return f"{parent}.{self.name}" if parent else self.name

    @property
    def resource_key(self) -> str:
        return self.full_name

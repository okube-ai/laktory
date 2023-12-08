from laktory.models.basemodel import BaseModel


class Secret(BaseModel):
    """
    Databricks secret

    Attributes
    ----------
    scope:
        Scope associated with the secret
    key:
        Key associated with the secret.
    value:
        Value associated with the secret
    """

    scope: str = None
    key: str = None
    value: str = None

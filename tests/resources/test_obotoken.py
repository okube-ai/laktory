from laktory.models.resources.databricks import OboToken

obo_token = OboToken(
    application_id="some_app",
)


def test_destinations():
    assert obo_token.application_id == "some_app"

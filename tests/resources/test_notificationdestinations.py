from laktory.models.resources.databricks import NotificationDestination

nd_emails = NotificationDestination(
    display_name="emails",
    config={"email": {"addresses": ["notifications@okube.ai"]}},
)


nd_teams = NotificationDestination(
    display_name="teams",
    config={"microsoft_teams": {"url": "teams.webhook.com"}},
)

nd_slack = NotificationDestination(
    display_name="slack",
    config={"slack": {"url": "slack.webhook.com"}},
)


def test_destinations():
    assert nd_emails.config.email.addresses == ["notifications@okube.ai"]
    assert nd_teams.config.microsoft_teams.url == "teams.webhook.com"
    assert nd_slack.config.slack.url == "slack.webhook.com"

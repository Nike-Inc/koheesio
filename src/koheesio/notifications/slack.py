"""
Classes to ease interaction with Slack
"""

from typing import Any, Dict, Optional
import datetime
import json
from textwrap import dedent

from koheesio.models import ConfigDict, Field
from koheesio.notifications import NotificationSeverity
from koheesio.steps.http import HttpPostStep
from koheesio.utils import utc_now


class SlackNotification(HttpPostStep):
    """
    Generic Slack notification class via the `Blocks` API

    > NOTE: `channel` parameter is used only with Slack Web API: https://api.slack.com/messaging/sending
        If webhook is used, the channel specification is not required

    Example:
    ```python
    s = SlackNotification(
        url="slack-webhook-url",
        channel="channel",
        message="Some *markdown* compatible text",
    )
    s.execute()
    ```
    """

    message: str = Field(default=..., description="The message that gets posted to Slack")
    channel: Optional[str] = Field(default=None, description="Slack channel id")
    headers: Optional[Dict[str, Any]] = {"Content-type": "application/json"}

    def get_payload(self) -> str:
        """
        Generate payload with `Block Kit`.
        More details: https://api.slack.com/block-kit
        """
        payload = {
            "attachments": [
                {
                    "blocks": [
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": self.message,
                            },
                        }
                    ],
                }
            ]
        }

        if self.channel:
            payload["channel"] = self.channel  # type: ignore[assignment]

        return json.dumps(payload)

    def execute(self) -> None:
        """
        Generate payload and send post request
        """
        self.data = self.get_payload()
        HttpPostStep.execute(self)


class SlackNotificationWithSeverity(SlackNotification):
    """
    Slack notification class via the `Blocks` API with etra severity information and predefined extra fields

    Example:
        from koheesio.steps.integrations.notifications import NotificationSeverity

        s = SlackNotificationWithSeverity(
            url="slack-webhook-url",
            channel="channel",
            message="Some *markdown* compatible text"
            severity=NotificationSeverity.ERROR,
            title="Title",
            environment="dev",
            application="Application"
        )
        s.execute()
    """

    severity: NotificationSeverity = Field(default=..., description="Severity of the message")
    title: str = Field(default=..., description="Title of your message")
    environment: str = Field(default=..., description="Environment description, e.g. dev / qa /prod")
    application: str = Field(default=..., description="Pipeline or application name")
    timestamp: datetime = Field(
        default_factory=utc_now,
        alias="execution_timestamp",
        description="Pipeline or application execution timestamp",
    )

    model_config = ConfigDict(use_enum_values=False)

    def get_payload_message(self) -> str:
        """
        Generate payload message based on the predefined set of parameters
        """
        return dedent(
            f"""
                {self.severity.alert_icon}   *{self.severity.name}:*  {self.title}
                *Environment:* {self.environment}
                *Application:* {self.application}
                *Message:* {self.message}
                *Timestamp:* {self.timestamp}
            """
        )

    def execute(self) -> None:
        """
        Generate payload and send post request
        """
        self.message = self.get_payload_message()
        self.data = self.get_payload()
        HttpPostStep.execute(self)

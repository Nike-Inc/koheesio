import json

from requests_mock.mocker import Mocker

from koheesio.notifications.slack import (
    NotificationSeverity,
    SlackNotification,
    SlackNotificationWithSeverity,
)

URL = "https://hooks.slack.com/services/foo/bar/baz"


def test_slack_notification():
    sn = SlackNotification(url=URL, channel="#cux", message="lorem ipsum")
    with Mocker() as rm:
        rm.post(URL, text="ok", status_code=int(200))
        sn.execute()
        assert sn.output.status_code == 200
        assert json.loads(sn.data)["channel"] == "#cux"
        assert json.loads(sn.data)["attachments"][0]["blocks"][0]["text"]["text"] == "lorem ipsum"


def test_slack_notification_wo_channel():
    sn = SlackNotification(url=URL, message="lorem ipsum")
    with Mocker() as rm:
        rm.post(URL, text="ok", status_code=int(200))
        sn.execute()
        assert sn.output.status_code == 200
        assert not json.loads(sn.data).get("channel")


def test_slack_notification_w_severity():
    sn = SlackNotificationWithSeverity(
        url=URL,
        channel="#cux",
        message="lorem ipsum",
        severity=NotificationSeverity.ERROR,
        title="Title",
        environment="local",
        application="Application",
    )
    with Mocker() as rm:
        rm.post(URL, text="ok", status_code=int(200))
        sn.execute()
        assert sn.output.status_code == 200
        assert ":red_circle:" in json.loads(sn.data)["attachments"][0]["blocks"][0]["text"]["text"]
        assert "*Message:* lorem ipsum" in json.loads(sn.data)["attachments"][0]["blocks"][0]["text"]["text"]

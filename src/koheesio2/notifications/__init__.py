"""Notification module for sending messages to notification services (e.g. Slack, Email, etc.)"""

from enum import Enum


class NotificationSeverity(Enum):
    """
    Enumeration of allowed message severities
    """

    INFO = "info"
    WARN = "warn"
    ERROR = "error"
    SUCCESS = "success"

    @property
    def alert_icon(self) -> str:
        """
        Return a colored circle in slack markup
        """
        return {
            NotificationSeverity.INFO: ":large_blue_circle:",
            NotificationSeverity.WARN: ":large_yellow_circle:",
            NotificationSeverity.ERROR: ":red_circle:",
            NotificationSeverity.SUCCESS: ":large_green_circle:",
        }.get(self, ":question:")

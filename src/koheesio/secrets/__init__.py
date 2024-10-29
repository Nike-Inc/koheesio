"""Module for secret integrations.

Contains abstract class for various secret integrations also known as SecretContext.
"""

from typing import Optional
from abc import ABC, abstractmethod

from koheesio import Step, StepOutput
from koheesio.context import Context
from koheesio.models import Field, SecretStr


class Secret(Step, ABC):
    """
    Abstract class for various secret integrations.
    All secrets are wrapped into Context class for easy access. Either existing context can be provided,
    or new context will be created and returned at runtime.

    Secrets are wrapped into the pydantic.SecretStr.
    """

    context: Optional[Context] = Field(
        Context({}),
        description="Existing `Context` instance can be used for secrets, otherwise new empty context will be created.",
    )
    root: Optional[str] = Field(default="secrets", description="All secrets will be grouped under this root.")
    parent: Optional[str] = Field(
        default=...,
        description="Group secrets from one secure path under this friendly name",
        pattern=r"^[a-zA-Z0-9_]+$",
    )  # Enforce regex to leverage the property style notation while working with Context: root.parent.secret

    class Output(StepOutput):
        """Output class for Secret."""

        context: Context = Field(default=..., description="Koheesio context")

    @classmethod
    def encode_secret_values(cls, data: dict) -> dict:
        """Encode secret values in the dictionary.

        Ensures that all values in the dictionary are wrapped in SecretStr.
        """
        encoded_dict = {}
        for key, value in data.items():
            if isinstance(value, dict):
                encoded_dict[key] = cls.encode_secret_values(value)
            else:
                encoded_dict[key] = SecretStr(value)  # type: ignore[assignment]
        return encoded_dict

    @abstractmethod
    def _get_secrets(self) -> dict:
        """
        Implement this method to return dictionary of protected secrets.
        """
        ...

    def execute(self) -> None:
        """
        Main method to handle secrets protection and context creation with "root-parent-secrets" structure.
        """
        context = Context(self.encode_secret_values(data={self.root: {self.parent: self._get_secrets()}}))
        self.output.context = self.context.merge(context=context)

    # noinspection PyMethodOverriding
    def get(self) -> Context:
        """
        Convenience method to return context with secrets.
        """
        self.execute()
        return self.output.context

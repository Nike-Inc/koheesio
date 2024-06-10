from typing import Optional, Union

import urllib3
from tableauserverclient import PersonalAccessTokenAuth, TableauAuth, Server

from abc import ABC
from koheesio.models import BaseModel

from typing import ContextManager
from pydantic import SecretStr, Field


class TableauBaseModel(BaseModel, ABC):
    url: str = Field(
        default=...,
        alias="url",
        description="Hostname for the Tableau server, e.g. tableau.my-org.com",
        examples=["tableau.my-org.com"],
    )
    user: str = Field(default=..., alias="user", description="Login name for the Tableau user")
    password: SecretStr = Field(default=..., alias="password", description="Password for the Tableau user")
    site_id: str = Field(
        default=...,
        alias="site_id",
        description="Identifier for the Tableau site, as used in the URL: https://tableau.my-org.com/#/site/SITE_ID",
    )
    version: str = Field(
        default="3.14",
        alias="version",
        description="Version of the Tableau server API",
    )
    token_name: Optional[str] = Field(
        default=None,
        alias="token_name",
        description="Name of the Tableau Personal Access Token",
    )
    token_value: Optional[SecretStr] = Field(
        default=None,
        alias="token_value",
        description="Value of the Tableau Personal Access Token",
    )

    # class Output(StepOutput):
    #     """
    #     Define outputs for the BoxFolderBase class
    #     """
    #
    #     folder: Optional[Folder] = Field(default=None, description="Box folder object")
    def __authenticate(self) -> ContextManager:
        """
        Authenticate on the Tableau server.

        Example
        -------
        ```python
        with self.__authenticate():
            # Do something with the authenticated session
        ```

        Returns
        -------
            TableauAuth or PersonalAccessTokenAuth authorization object
        """
        # Suppress 'InsecureRequestWarning'
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        tableau_auth = TableauAuth(username=self.user, password=self.password, site_id=self.site_id)

        if self.token_name and self.token_value:
            self.log.info(
                "Token details provided, this will take precedence over username and password authentication."
            )
            tableau_auth = PersonalAccessTokenAuth(
                token_name=self.token_name, personal_access_token=self.token_value, site_id=self.site_id
            )

        server = Server(self.url)
        server.version = self.version
        server.add_http_options({"verify": False})

        with server.auth.sign_in(tableau_auth):
            # TODO: logging and check if authorized
            if not self.site_id:
                raise ValueError("Invalid credentials. Cannot create authorization to connect to Tableau Server.")

        self.log.debug(f"Authorized in Tableau Server `{self.url}` and site `{self.site_id}`")

        return server.auth.sign_in(tableau_auth)

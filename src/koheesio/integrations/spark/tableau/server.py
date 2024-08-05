from typing import Optional, Union

import os
import urllib3
from abc import ABC, abstractmethod
from pathlib import PurePath
from tableauserverclient import PersonalAccessTokenAuth, TableauAuth, Server, ProjectItem, Pager, DatasourceItem

from koheesio.steps import Step, StepOutput
from koheesio.models import model_validator, field_validator

from typing import ContextManager
from pydantic import SecretStr, Field


class TableauServer(Step):
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
    project: Optional[str] = Field(
        default=None,
        alias="project",
        description="Name of the project on the Tableau server",
    )
    parent_project: Optional[str] = Field(
        default=None,
        alias="parent_project",
        description="Name of the parent project on the Tableau server, use 'root' for the root project.",
    )
    project_id: Optional[str] = Field(
        default=None,
        alias="project_id",
        description="ID of the project on the Tableau server",
    )

    # @model_validator(mode="before")
    # def validate_project(cls, data: dict) -> dict:
    #     """Validate the path, make sure path and file_name are Path objects."""
    #     project = data.get("project")
    #     project_id = data.get("project_id")
    #
    #     if project and project_id:
    #         raise ValueError("Both 'project' and 'project_id' parameters cannot be provided at the same time.")

    def __authenticate(self) -> ContextManager:
        """
        Authenticate on the Tableau server.

        Example:
            with self.__authenticate():

        Returns:
            TableauAuth or PersonalAccessTokenAuth authorization object
        """
        # Suppress 'InsecureRequestWarning'
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        tableau_auth = TableauAuth(username=self.user, password=self.password.get_secret_value(), site_id=self.site_id)

        if self.token_name and self.token_value:
            self.log.info(
                "Token details provided, this will take precedence over username and password authentication."
            )
            tableau_auth = PersonalAccessTokenAuth(
                token_name=self.token_name, personal_access_token=self.token_value, site_id=self.site_id
            )

        self.__server = Server(self.url)
        self.__server.version = self.version
        self.__server.add_http_options({"verify": False})

        with self.__server.auth.sign_in(tableau_auth):
            # TODO: logging and check if authorized
            if not self.site_id:
                raise ValueError("Invalid credentials. Cannot create authorization to connect to Tableau Server.")

        self.log.debug(f"Authorized in Tableau Server `{self.url}` and site `{self.site_id}`")

        return self.__server.auth.sign_in(tableau_auth)

    @property
    def working_project(self) -> Union[ProjectItem, None]:
        """
        Identify working project by using `project` and `parent_project` (if necessary) class properties.
        The goal is to uniquely identify specific project on the server, if multiple projects have the same
        name, the `parent_project` attribute of the TableauServer is required.

        If `id` of the project is known, it can be used in `project_id` parameter, then the detection of the working
        project using the `project` and `parent_project` attributes is skipped.

        Returns:
            ProjectItem: ProjectItem object representing the working project
        """

        with self.__authenticate():
            all_projects = Pager(self._server.projects)
            parent, lim_p = None, []

            for project in all_projects:
                if project.id == self.project_id:
                    lim_p = [project]
                    self.log.info(
                        f"\nProject ID provided directly:\n\tName: {lim_p[0].name}\n\tID: {lim_p[0].id}"
                    )
                    break

                # Identify parent project
                if project.name.strip() == self.parent_project and not self.project_id:
                    parent = project
                    self.log.info(
                        f"\nParent project identified:\n\tName: {parent.name}\n\tID: {parent.id}"
                    )

                # Identify project(s)
                if project.name.strip() == self.project and not self.project_id:
                    lim_p.append(project)

            # Further filter the list of projects by parent project id
            if self.parent_project == "root" and not self.project_id:
                lim_p = [p for p in lim_p if p.parent_id == 0]
            elif self.parent_project and parent and not self.project_id:
                lim_p = [p for p in lim_p if p.parent_id == parent.id]

            if len(lim_p) > 1:
                raise ValueError(
                    "Multiple projects with the same name exist on the server, "
                    "please provide `parent_project` attribute."
                )
            elif len(lim_p) == 0:
                raise ValueError("Working project could not be identified.")
            else:
                self.log.info(
                    f"\nWorking project identified:\n\tName: {lim_p[0].name}\n\tID: {lim_p[0].id}"
                )
                return lim_p[0]

    def execute(self):
        raise NotImplementedError("Method `execute` must be implemented in the subclass.")


class TableauHyperPublisher(TableauServer):
    datasource_name: str = Field(default=..., description="Name of the datasource to publish")
    hyper_path: PurePath = Field(default=..., description="Path to Hyper file")
    publish_mode: Server.PublishMode = Field(
        default=Server.PublishMode.Overwrite,
        description="Publish mode for the Hyper file",
    )

    class Output(StepOutput):
        """Output class for HyperFileListWriter"""
        datasource_item: DatasourceItem = Field(
            default=...,
            description="DatasourceItem object representing the published datasource"
        )

    @field_validator("publish_mode")
    def validate_publish_mode(cls, value: str) -> str:
        """
        Only allow "Append" and "Overwrite" publish modes.
        """
        if value not in [Server.PublishMode.Append, Server.PublishMode.Overwrite]:
            raise ValueError(f"Only {Server.PublishMode.Append} and {Server.PublishMode.Overwrite} are allowed")

        return value

    def execute(self):
        # Ensure that the Hyper File exists
        if not os.path.isfile(self.hyper_path):
            raise FileNotFoundError(f"Hyper file not found at: {self.hyper_path.as_posix()}")

        with self.__authenticate():
            # Finally, publish the Hyper File to the Tableau server
            self.log.info(f'Publishing Hyper File located at: "{self.hyper_path.as_posix()}"')
            self.log.debug(f"Create mode: {self.publish_mode}")

            datasource_item = self.__server.datasources.publish(
                datasource_item=DatasourceItem(project_id=self.working_project.id, name=self.datasource_name),
                file=self.hyper_path.as_posix(),
                mode=self.publish_mode,
            )
            self.log.info(f"Published datasource to Tableau server with the id: {datasource_item.id}")

            self.output.datasource_item = datasource_item

    def publish(self):
        self.execute()

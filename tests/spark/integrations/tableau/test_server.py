from datetime import datetime
from pathlib import PurePath, Path

import pytest

from koheesio.integrations.spark.tableau.server import TableauServer, TableauHyperPublisher

pytestmark = pytest.mark.spark


class TestServer:
    @pytest.fixture(autouse=False)
    def server(self, mocker):
        __server = mocker.patch("koheesio.integrations.spark.tableau.server.Server")
        __mock_server = __server.return_value

        from koheesio.integrations.spark.tableau.server import TableauServer, TableauHyperPublisher

        # Mocking various returns from the Tableau server
        def create_mock_object(name_prefix: str, object_id: int):
            obj = mocker.MagicMock()
            obj.name = f"{name_prefix}-{object_id}"
            obj.project_id = f"{object_id}"
            return obj

        def create_mock_pagination(length: int):
            obj = mocker.MagicMock()
            obj.total_available = length
            return obj

        # Projects
        mock_projects = []
        mock_projects_pagination = mocker.MagicMock()

        def create_mock_project(
                name_prefix: str, project_id: int, parent_id: int = None
        ):
            mock_project = mocker.MagicMock()
            mock_project.name = f"{name_prefix}-{project_id}"
            mock_project.id = (
                f"{project_id}" if not parent_id else f"{project_id * parent_id}"
            )
            mock_project.parent_id = f"{parent_id}"
            return mock_project

        # Parent Projects
        for i in range(1, 3):
            mock_projects.append(
                create_mock_project(name_prefix="parent-project", project_id=i)
            )
            # Child Projects
            r = range(3, 5) if i == 1 else range(4, 6)
            for ix in r:
                mock_projects.append(
                    create_mock_project(
                        name_prefix="project", project_id=ix, parent_id=i
                    )
                )

        mock_projects_pagination.total_available = len(mock_projects)

        __mock_server.projects.get.return_value = [
            mock_projects,
            mock_projects_pagination,
        ]

        # Data Sources
        mock_ds = create_mock_object("datasource", 1)
        mock_conn = create_mock_object("connection", 1)
        mock_conn.type = "baz"
        mock_ds.connections = [mock_conn]
        __mock_server.datasources.get.return_value = [
            [mock_ds],
            create_mock_pagination(1),
        ]

        # Workbooks
        __mock_server.workbooks.get.return_value = [
            [create_mock_object("workbook", 1)],
            create_mock_pagination(1),
        ]

        yield TableauServer(
            url="https://tableau.domain.com",
            user="user",
            password="pass",
            site_id="site",
        )


    def test__get_working_project__not_set(self, server):
        assert not server.working_project

    def test__get_working_project__id(self):
        r = self.tableau_wrapper._get_working_project(project_id="3")
        assert r.id == "3" and r.name == "project-3"

    def test___get_working_project__multiple_projects(self):
        with pytest.raises(self.tableau_wrapper.MultipleWorkingProjectsException):
            self.tableau_wrapper.project = "project-4"
            self.tableau_wrapper._get_working_project()

    def test__get_working_project__project_and_parent(self):
        self.tableau_wrapper.project = "project-4"
        self.tableau_wrapper.parent_project = "parent-project-1"
        r = self.tableau_wrapper._get_working_project()
        assert r.id == "4" and r.name == "project-4"

    def test___get_working_project__project(self):
        self.tableau_wrapper.project = "project-5"
        r = self.tableau_wrapper._get_working_project()
        assert r.id == "10" and r.name == "project-5"

    def test___get_working_project__unknown_project(self):
        with pytest.raises(self.tableau_wrapper.UnidentifiedWorkingProjectException):
            self.tableau_wrapper.project = "project-6"
            self.tableau_wrapper._get_working_project()

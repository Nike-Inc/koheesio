from typing import Any

import pytest
from tableauserverclient import DatasourceItem

from koheesio.integrations.spark.tableau.server import TableauHyperPublisher


class TestTableauServer:
    @pytest.fixture(autouse=False)
    def server(self, mocker):
        __server = mocker.patch("koheesio.integrations.spark.tableau.server.Server")
        __mock_server = __server.return_value

        from koheesio.integrations.spark.tableau.server import TableauServer

        # Mocking various returns from the Tableau server
        def create_mock_object(name_prefix: str, object_id: int, spec: Any = None, project_id: int = None):
            obj = mocker.MagicMock() if not spec else mocker.MagicMock(spec=spec)
            obj.id = f"{object_id}"
            obj.name = f"{name_prefix}-{object_id}"
            obj.project_id = f"{project_id}" if project_id else None
            return obj

        def create_mock_pagination(length: int):
            obj = mocker.MagicMock()
            obj.total_available = length
            return obj

        # Projects
        mock_projects = []
        mock_projects_pagination = mocker.MagicMock()

        def create_mock_project(name_prefix: str, project_id: int, parent_id: int = None):
            mock_project = mocker.MagicMock()
            mock_project.name = f"{name_prefix}-{project_id}"
            mock_project.id = f"{project_id}" if not parent_id else f"{project_id * parent_id}"
            mock_project.parent_id = f"{parent_id}" if parent_id else None
            return mock_project

        # Parent Projects
        for i in range(1, 3):
            mock_projects.append(create_mock_project(name_prefix="parent-project", project_id=i))
            # Child Projects
            r = range(3, 5) if i == 1 else range(4, 6)
            for ix in r:
                mock_projects.append(create_mock_project(name_prefix="project", project_id=ix, parent_id=i))

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

        __mock_server.datasources.publish.return_value = create_mock_object(
            "published_datasource", 1337, spec=DatasourceItem
        )

        yield TableauServer(
            url="https://tableau.domain.com", user="user", password="pass", site_id="site", project_id="1"
        )

    @pytest.fixture()
    def hyper_file(self, data_path):
        return f"{data_path}/readers/hyper_file/dummy.hyper"

    def test_working_project_w_project_id(self, server):
        server.project_id = "3"
        wp = server.working_project
        assert wp.id == "3" and wp.name == "project-3"

    def test_working_project_w_project_name(self, server):
        server.project_id = None
        server.project = "project-5"
        wp = server.working_project
        assert wp.id == "10" and wp.name == "project-5"

    def test_working_project_w_project_name_and_parent_project(self, server):
        server.project_id = None
        server.project = "project-4"
        server.parent_project = "parent-project-1"
        wp = server.working_project
        assert wp.id == "4" and wp.name == "project-4"

    def test_working_project_w_project_name_and_root(self, server):
        server.project_id = None
        server.project = "parent-project-1"
        server.parent_project = "root"
        wp = server.working_project
        assert wp.id == "1" and wp.name == "parent-project-1"

    def test_working_project_multiple_projects(self, server):
        with pytest.raises(ValueError):
            server.project_id = None
            server.project = "project-4"
            server.working_project

    def test_working_project_unknown(self, server):
        with pytest.raises(ValueError):
            server.project_id = None
            server.project = "project-6"
            server.working_project

    def test_publish_hyper(self, server, hyper_file):
        p = TableauHyperPublisher(
            url="https://tableau.domain.com",
            user="user",
            password="pass",
            site_id="site",
            project_id="1",
            hyper_path=hyper_file,
            datasource_name="published_datasource",
        )
        p.publish()
        assert p.output.datasource_item.id == "1337"

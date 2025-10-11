from collections import namedtuple
from io import BytesIO
from pathlib import PurePath

import pytest

from pydantic import ValidationError

from koheesio.integrations.box import (
    Box,
    BoxBufferFileWriter,
    BoxCsvFileReader,
    BoxCsvPathReader,
    BoxFileBase,
    BoxFileWriter,
    BoxFolderBase,
    BoxFolderCreate,
    BoxFolderDelete,
    BoxFolderGet,
    BoxFolderNotFoundError,
    BoxPathIsEmptyError,
    File,
    Folder,
    JWTAuth,
    SecretStr,
    StructType,
)
from koheesio.spark.writers.buffer import PandasCsvBufferWriter

pytestmark = pytest.mark.spark

COMMON_PARAMS = {
    "client_id": SecretStr("client_id"),
    "client_secret": SecretStr("client_secret"),
    "enterprise_id": SecretStr("enterprise_id"),
    "jwt_key_id": SecretStr("jwt_key_id"),
    "rsa_private_key_data": SecretStr("rsa_private_key_data"),
    "rsa_private_key_passphrase": SecretStr("rsa_private_key_passphrase"),
}
CUSTOM_SCHEMA = StructType.fromJson(
    {
        "fields": [
            {"metadata": {}, "name": "foo", "nullable": True, "type": "string"},
            {"metadata": {}, "name": "bar", "nullable": True, "type": "integer"},
        ],
        "type": "struct",
    }
)


@pytest.fixture(autouse=True)
def dummy_box(mocker):
    class DummyBox:
        def __init__(self, *args, **kwargs):
            self.args = args

        def _mock_folder(self, name: str, object_id: str, get_items: list):
            mf = mocker.Mock(spec=Folder)
            mf.object_id = object_id
            mf.name = name
            mf.type = "folder"
            mf.get_items.return_value = get_items
            mf.create_subfolder.return_value = mocker.Mock(type="folder", object_id="999", spec=Folder)
            mf.upload_stream.side_effect = self._mock_file
            return mf

        @staticmethod
        def _mock_file(name: str = None, object_id: str = "0", content: bytes = None, **kwargs):
            mf = mocker.Mock(spec=File)
            mf.object_id = object_id
            mf.name = name if name else kwargs["file_name"]
            mf.type = "file"
            mf.content.return_value = content
            Properties = namedtuple("Properties", "name")
            mf.get.return_value = Properties(name=mf.name)
            mf.get_shared_link.return_value = "https://my-ent.box.com/dummy"
            return mf

        def folder(self, *args, **kwargs):
            """
            Mock the following folder and file structure:
                root/
                ├─ bar/
                ├─ foo/
                │  ├─ f1.csv
                │  ├─ f2.csv
                │  ├─ f3.xlsx
            """
            mock_foo = self._mock_folder(
                name="foo",
                object_id="1",
                get_items=[
                    self._mock_file(name="f1.csv", object_id="10"),
                    self._mock_file(name="f2.csv", object_id="20"),
                    self._mock_file(name="f3.xlsx", object_id="30"),
                ],
            )
            mock_bar = self._mock_folder(
                name="bar",
                object_id="2",
                get_items=[],
            )

            mock_root = self._mock_folder(name="root", object_id="0", get_items=[mock_foo, mock_bar])

            if kwargs["folder_id"] == "0":
                return mock_root

            if kwargs["folder_id"] == "1":
                return mock_foo

        def file(self, *args, **kwargs):
            if kwargs["file_id"] == "10":
                return self._mock_file(name="f1.csv", object_id="10", content=b"foo,bar\nA,1")
            if kwargs["file_id"] == "20":
                return self._mock_file(name="f2.csv", object_id="20", content=b"foo,bar\nA,2")
            if kwargs["file_id"] == "30":
                return self._mock_file(name="upload.csv", object_id="30", content=b"foo,bar\nA,2")

    mocker.patch("koheesio.integrations.box.JWTAuth", spec=JWTAuth)
    mocker.patch("koheesio.integrations.box.Client", new=DummyBox)


class TestBox:
    def test__init__(self, dummy_box):
        b = Box(**COMMON_PARAMS)
        assert "JWTAuth" in str(b.client.args[0])


class TestBoxFolderBase:
    def test_validate_folder_or_path_w_both(self):
        with pytest.raises(ValidationError):
            BoxFolderBase(folder="0", path="foo")

    def test_validate_folder_or_path_wo_both(self):
        with pytest.raises(ValidationError):
            BoxFolderBase()

    def test_action(self):
        with pytest.raises(NotImplementedError):
            BoxFolderBase(**COMMON_PARAMS, folder="0").action()


class TestBoxFolderGet:
    def test_get_w_default_root(self, dummy_box):
        folder = (
            BoxFolderGet(
                **COMMON_PARAMS,
                path="/foo",
                root="0",
            )
            .execute()
            .folder
        )
        assert folder.name == "foo"
        assert len(folder.get_items()) == 3

    def test_get_w_folder_and_path(self, dummy_box):
        with pytest.raises(AttributeError):
            BoxFolderGet(
                **COMMON_PARAMS,
                folder="1",
                path="/foo",
            ).execute().folder

    def test_get_w_custom_root(self, dummy_box):
        folder = (
            BoxFolderGet(
                **COMMON_PARAMS,
                path="/",
                root="1",
            )
            .execute()
            .folder
        )
        assert folder.name == "foo"
        assert len(folder.get_items()) == 3

    def test_get_w_invalid_path(self, dummy_box):
        with pytest.raises(BoxFolderNotFoundError):
            (
                BoxFolderGet(
                    **COMMON_PARAMS,
                    path="/buz",
                    root="0",
                )
                .execute()
                .folder
            )


class TestBoxFolderCreate:
    def test_create_w_path(
        self,
        dummy_box,
    ):
        folder = (
            BoxFolderCreate(
                **COMMON_PARAMS,
                path="/foo/buz",
            )
            .execute()
            .folder
        )
        assert folder.object_id == "999"

    def test_create_w_folder(self, dummy_box):
        with pytest.raises(AttributeError):
            BoxFolderCreate(**COMMON_PARAMS, folder="0")


class TestBoxFolderDelete:
    def test_execute(self, dummy_box):
        BoxFolderDelete(**COMMON_PARAMS, path="/foo/buz")


class TestBoxFileBase:
    def test_execute(self, mocker):
        action = mocker.patch.object(BoxFileBase, "action")
        BoxFileBase(**COMMON_PARAMS, files=["10", "20"], path="/foo").execute()
        assert action.call_count == 2
        assert action.call_args_list[0].kwargs["file"].name == "f1.csv"
        assert action.call_args_list[0].kwargs["folder"].name == "foo"


class TestBoxCsvReader:
    def test_execute_w_schema(self, spark, dummy_box):
        bcr = BoxCsvFileReader(
            **COMMON_PARAMS,
            file=["10"],
            schema=CUSTOM_SCHEMA,
        ).execute()
        assert bcr.df.count() == 1
        assert bcr.df.dtypes == [
            ("foo", "string"),
            ("bar", "int"),
            ("meta_file_id", "string"),
            ("meta_file_name", "string"),
            ("meta_load_timestamp", "timestamp"),
        ]

    def test_execute_wo_schema(self, spark, dummy_box):
        bcr = BoxCsvFileReader(
            **COMMON_PARAMS,
            file=["10"],
        ).execute()
        assert bcr.df.count() == 1
        assert bcr.df.dtypes == [
            ("foo", "string"),
            ("bar", "bigint"),
            ("meta_file_id", "string"),
            ("meta_file_name", "string"),
            ("meta_load_timestamp", "timestamp"),
        ]

    def test_file_attribute_validation(self, dummy_box):
        """Tests that if one single file id is provided, it will be converted to a list"""
        bcr = BoxCsvFileReader(**COMMON_PARAMS, file="this-is-a-single-file-id")
        assert bcr.files == ["this-is-a-single-file-id"]


class TestBoxCsvPathReader:
    def test_execute_w_files(self, spark, dummy_box):
        bcr = BoxCsvPathReader(
            **COMMON_PARAMS,
            path="/foo",
            schema=CUSTOM_SCHEMA,
        ).execute()
        assert bcr.df.count() == 2
        assert bcr.df.dtypes == [
            ("foo", "string"),
            ("bar", "int"),
            ("meta_file_id", "string"),
            ("meta_file_name", "string"),
            ("meta_load_timestamp", "timestamp"),
        ]

    def test_execute_wo_files(self, spark, dummy_box):
        with pytest.raises(BoxPathIsEmptyError):
            BoxCsvPathReader(
                **COMMON_PARAMS,
                path="/bar",
            ).execute()


class TestBoxFileWriter:
    def test_execute_bytesio_wo_file_name(self, dummy_box):
        with pytest.raises(AttributeError):
            BoxFileWriter(**COMMON_PARAMS, path="/foo", file=BytesIO(b"my-file")).execute()

    def test_execute_bytesio(self, dummy_box):
        f = BoxFileWriter(
            **COMMON_PARAMS,
            path="/foo",
            file=BytesIO(b"my-data"),
            file_name="upload.txt",
        ).execute()
        assert f.shared_link
        assert isinstance(f.file, File)
        assert f.file.get().name == "upload.txt"

    def test_execute_file_wo_file_name(self, dummy_box):
        # file_name not set, will default to original file name
        f = BoxFileWriter(**COMMON_PARAMS, path="/foo", file=__file__).execute()
        assert f.file.get().name == PurePath(__file__).name

    def test_execute_file_w_file_name(self, dummy_box):
        # file_name set, will override original file name
        f = BoxFileWriter(
            **COMMON_PARAMS,
            path="/foo",
            file=__file__,
            file_name="upload.txt",
        ).execute()
        assert f.file.get().name == "upload.txt"


class TestBoxBufferFileWriter:
    def test_execute(self, dummy_box, spark):
        df = spark.createDataFrame([("A", 1), ("B", 2)], ["foo", "bar"])
        buffer_writer = PandasCsvBufferWriter(df=df)

        f = BoxBufferFileWriter(
            **COMMON_PARAMS,
            path="/foo",
            buffer_writer=buffer_writer,
            file_name="upload.txt",
        ).execute()

        assert f.shared_link
        assert isinstance(f.file, File)
        assert f.file.get().name == "upload.txt"

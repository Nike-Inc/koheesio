from unittest import mock

import paramiko
from paramiko import SSHException
import pytest

from koheesio.integrations.spark.sftp import (
    SendCsvToSftp,
    SendJsonToSftp,
    SFTPWriteMode,
    SFTPWriter,
)
from koheesio.spark.writers.buffer import PandasCsvBufferWriter

pytestmark = pytest.mark.spark


@pytest.fixture
def buffer_writer(spark):
    # Create a small Spark DataFrame for testing
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["column1", "column2"])
    return PandasCsvBufferWriter(df=df)


expected_data = "column1,column2\n1,a\n2,b\n"


@pytest.fixture(scope="session")
def sftp_fixture(sftpserver):
    # https://github.com/ulope/pytest-sftpserver/issues/30
    sftpserver.daemon_threads = True
    sftpserver.block_on_close = False
    yield sftpserver


@pytest.fixture(scope="function")
def prepare_sftp(sftp_fixture):
    # create empty files since the sftpserver doesn't allow to create and write to file
    with sftp_fixture.serve_content(
        {
            "test_dir": {
                "overwrite": {"test.csv": "existing_data"},
                "append": {"test.csv": "existing_data"},
                "ignore": {"test.csv": "existing_data"},
                "exclusive": {"test.csv": "existing_data"},
                "backup": {"test.csv": "existing_data"},
                "update_diff_data": {"test.csv": "existing_data"},
                "update_same_data": {"test.csv": expected_data},
            }
        }
    ):
        yield


def read_sftp_file(sftp_fixture, file_path):
    data = None
    sftp = None
    transport = None
    try:
        # Create a new transport and connect to the SFTP server
        transport = paramiko.Transport((sftp_fixture.host, sftp_fixture.port))
        transport.connect(username="a", password="b")

        # Create an SFTP client from the transport
        sftp = paramiko.SFTPClient.from_transport(transport)

        # Try to read the test data from the server
        with sftp.open(file_path, "r") as file:
            data = file.read().decode("utf-8")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the client and transport
        if sftp:
            sftp.close()
        if transport:
            transport.close()

    return data


def test_canary(sftp_fixture, prepare_sftp):
    """sftpserver setup canary test"""
    actual_data = read_sftp_file(sftp_fixture, "/test_dir/overwrite/test.csv")
    assert actual_data == "existing_data"


def test_invalid_write_mode():
    """Test that an error is raised for an invalid write mode"""
    with pytest.raises(ValueError):
        SFTPWriter(
            buffer_writer=PandasCsvBufferWriter(),
            mode="invalid_mode",
            host="localhost",
            port=22,
            username="a",
            password="b",
            path="/test.csv",
        )


def test_connection_failure(sftp_fixture, buffer_writer):
    """Test that an error is raised when the connection to the SFTP server fails"""
    with mock.patch("paramiko.Transport.connect", side_effect=SSHException("Could not connect")):
        with pytest.raises(SSHException):
            writer = SFTPWriter(
                buffer_writer=buffer_writer,
                host=sftp_fixture.host,
                port=sftp_fixture.port,
                username="a",
                password="b",
                path="/test.csv",
            )
            writer.execute()


def test_file_read_failure(sftp_fixture, buffer_writer):
    """Test that an error is raised when reading a file from the SFTP server fails"""
    writer = SFTPWriter(
        buffer_writer=buffer_writer,
        mode="overwrite",
        host=sftp_fixture.host,
        port=sftp_fixture.port,
        username="a",
        password="b",
        path="/test.csv",
    )
    buffer_output = buffer_writer.output.read()
    with mock.patch.object(writer.client, "open", side_effect=IOError("Could not read file")):
        with pytest.raises(IOError):
            writer._handle_write_mode("/test.csv", buffer_output)


def test_file_write_failure(sftp_fixture, buffer_writer):
    """Test that an error is raised when writing a file to the SFTP server fails"""
    writer = SFTPWriter(
        buffer_writer=buffer_writer,
        host=sftp_fixture.host,
        port=sftp_fixture.port,
        username="a",
        password="b",
        path="/test.csv",
    )
    with mock.patch.object(writer, "write_file", side_effect=IOError("Could not write file")):
        with pytest.raises(IOError):
            writer.execute()


def test_execute_overwrite_mode(sftp_fixture, prepare_sftp, buffer_writer):
    # SetUp
    path = "/test_dir/overwrite/test.csv"
    writer = SFTPWriter(
        buffer_writer=buffer_writer,
        mode=SFTPWriteMode.OVERWRITE,
        host=sftp_fixture.host,
        port=sftp_fixture.port,
        username="a",
        password="b",
        path=path,
    )

    # Act
    writer.execute()

    # Assert
    actual_data = read_sftp_file(sftp_fixture, path)
    assert actual_data == expected_data


def test_execute_append_mode(sftp_fixture, prepare_sftp, buffer_writer):
    # SetUp
    path = "/test_dir/append/test.csv"
    writer = SFTPWriter(
        buffer_writer=buffer_writer,
        mode=SFTPWriteMode.APPEND,
        host=sftp_fixture.host,
        port=sftp_fixture.port,
        username="a",
        password="b",
        path=path,
    )

    # Act
    writer.execute()

    # Assert
    actual_data = read_sftp_file(sftp_fixture, path)
    assert actual_data == "existing_data" + expected_data


def test_execute_ignore_mode_existing_file(sftp_fixture, prepare_sftp, buffer_writer):
    # SetUp
    path = "/test_dir/ignore/test.csv"

    writer = SFTPWriter(
        buffer_writer=buffer_writer,
        mode=SFTPWriteMode.IGNORE,
        host=sftp_fixture.host,
        port=sftp_fixture.port,
        username="a",
        password="b",
        path=path,
    )

    # Act
    writer.execute()

    # Assert
    actual_data = read_sftp_file(sftp_fixture, path)
    assert actual_data == "existing_data"  # The existing data should not be overwritten


def test_execute_exclusive_mode_existing_file(sftp_fixture, prepare_sftp, buffer_writer):
    # SetUp
    path = "/test_dir/exclusive/test.csv"

    writer = SFTPWriter(
        buffer_writer=buffer_writer,
        mode=SFTPWriteMode.EXCLUSIVE,
        host=sftp_fixture.host,
        port=sftp_fixture.port,
        username="a",
        password="b",
        path=path,
    )

    # Act & Assert
    with pytest.raises(FileExistsError):
        writer.execute()


def test_execute_exclusive_mode_new_file(sftp_fixture, prepare_sftp, buffer_writer):
    # SetUp
    path = "/test_dir/exclusive/new_file.csv"

    writer = SFTPWriter(
        buffer_writer=buffer_writer,
        mode=SFTPWriteMode.EXCLUSIVE,
        host=sftp_fixture.host,
        port=sftp_fixture.port,
        username="a",
        password="b",
        path=path,
    )

    # Act
    writer.execute()

    # Assert
    actual_data = read_sftp_file(sftp_fixture, path)
    assert actual_data == expected_data


def test_execute_backup_mode_existing_file(sftp_fixture, prepare_sftp, buffer_writer):
    # SetUp
    path = "/test_dir/backup/test.csv"

    writer = SFTPWriter(
        buffer_writer=buffer_writer,
        mode=SFTPWriteMode.BACKUP,
        host=sftp_fixture.host,
        port=sftp_fixture.port,
        username="a",
        password="b",
        path=path,
    )

    # Act
    writer.execute()

    # Assert
    actual_data = read_sftp_file(sftp_fixture, path)
    assert actual_data == expected_data  # The existing data should be overwritten

    # Assert backup file is created
    transport = paramiko.Transport((sftp_fixture.host, sftp_fixture.port))
    transport.connect(username="a", password="b")

    sftp = None
    try:
        # Create an SFTP client from the transport
        sftp = paramiko.SFTPClient.from_transport(transport)

        # List the files in the directory
        files = sftp.listdir("/test_dir/backup")

        # Check if there is a file that matches the backup file pattern
        backup_files = [f for f in files if f.endswith(".bak")]
        assert len(backup_files) == 1

    finally:
        # Close the client and transport
        if sftp:
            sftp.close()
        transport.close()


def test_execute_update_mode_existing_file_same_data(sftp_fixture, prepare_sftp, buffer_writer):
    # SetUp
    path = "/test_dir/update_same_data/test.csv"

    writer = SFTPWriter(
        buffer_writer=buffer_writer,
        mode=SFTPWriteMode.UPDATE,
        host=sftp_fixture.host,
        port=sftp_fixture.port,
        username="a",
        password="b",
        path=path,
    )

    # Act
    writer.execute()

    # Assert
    actual_data = read_sftp_file(sftp_fixture, path)
    assert actual_data == expected_data  # The existing data should not be overwritten


def test_execute_update_mode_existing_file_different_data(sftp_fixture, prepare_sftp, buffer_writer):
    # SetUp
    path = "/test_dir/update_diff_data/test.csv"

    writer = SFTPWriter(
        buffer_writer=buffer_writer,
        mode=SFTPWriteMode.UPDATE,
        host=sftp_fixture.host,
        port=sftp_fixture.port,
        username="a",
        password="b",
        path=path,
    )

    # Act
    writer.execute()

    # Assert
    actual_data = read_sftp_file(sftp_fixture, path)
    assert actual_data == expected_data  # The existing data should be overwritten


def test_write_method(sftp_fixture, prepare_sftp, spark):
    """Test that the write method works as expected"""
    # SetUp
    path = "/test_dir/overwrite/test.csv"

    writer = SFTPWriter(
        buffer_writer=PandasCsvBufferWriter(),  # We use a PandasCsvBufferWriter with no data and default parameters
        mode=SFTPWriteMode.OVERWRITE,
        host=sftp_fixture.host,
        port=sftp_fixture.port,
        username="a",
        password="b",
        path=path,
    )

    df = spark.createDataFrame([(1, "a"), (2, "b")], ["column1", "column2"])
    _expected_data = "column1,column2\n1,a\n2,b\n"

    # Act
    writer.write(df)

    # Assert
    actual_data = read_sftp_file(sftp_fixture, path)
    assert actual_data == _expected_data


def test_write_mode_passed_as_string():
    """Test that the write mode can be passed as a string"""
    # SetUp
    writer = SFTPWriter(
        buffer_writer=PandasCsvBufferWriter(),
        mode="overwrite",
        host="localhost",
        port=22,
        username="a",
        password="b",
        path="/test.csv",
    )

    # Act & Assert
    assert writer.mode == SFTPWriteMode.OVERWRITE
    assert writer.write_mode == "wb"


def test_path_and_filename_process_correctly():
    """Test that the path and filename are processed correctly"""
    # SetUp
    writer = SFTPWriter(
        buffer_writer=PandasCsvBufferWriter(),
        mode="overwrite",
        host="goodbye",
        port=42,
        path="/test_dir/",
        filename="thanks_for_all_the_fish.csv",  # we use the alias for file_name explicitly
    )

    # Act & Assert
    assert writer.file_name == "thanks_for_all_the_fish.csv"
    assert writer.path.as_posix() == "/test_dir/thanks_for_all_the_fish.csv"


def test_sftp_removed_from_host_name():
    """Test that the SFTP server is removed from the host after the test"""
    # SetUp
    host = "sftp://localhost/"
    writer = SFTPWriter(
        buffer_writer=PandasCsvBufferWriter(),
        host=host,
        port=42,
        path="/test.csv",
    )

    # Assert
    assert writer.host == "localhost"


def test_invalid_path():
    """Test that an error is raised when an invalid path is passed"""
    with pytest.raises(ValueError):
        SFTPWriter(
            buffer_writer=PandasCsvBufferWriter(),
            host="localhost",
            port=22,
            path=123.456,
        )


def test_send_csv_to_sftp(sftp_fixture, prepare_sftp, spark):
    """Test that the write method works as expected"""
    # SetUp
    path = "/test_dir/overwrite/test.csv"
    test_data = [
        ("foo", 1),
        ("bar", 2),
        ("baz", 3),
    ]
    test_schema = ["name", "value"]

    writer = SendCsvToSftp(
        # Parameters for the SFTPWriter part
        host=sftp_fixture.host,
        port=sftp_fixture.port,
        username="a",
        password="b",
        path=path,
        # Parameters for the PandasCsvBufferWriter part
        sep="\t",
        line_terminator="\r",
        header=False,
        columns=test_schema,
    )

    _df = spark.createDataFrame(test_data, test_schema)
    _expected_data = "foo\t1\rbar\t2\rbaz\t3\r"

    # Act
    writer.write(_df)

    # Assert that the PandasCsvBufferWriter parameters are set correctly
    assert writer.buffer_writer.sep == "\t"
    assert writer.buffer_writer.lineSep == "\r"  # line_terminator is an alias for lineSep
    assert writer.buffer_writer.header is False
    assert writer.buffer_writer.columns == test_schema

    # Assert that the data was written to the SFTP server
    actual_data = read_sftp_file(sftp_fixture, path)
    assert actual_data == _expected_data


def test_send_json_to_sftp(sftp_fixture, prepare_sftp, spark):
    """Test that the write method works as expected"""
    # SetUp
    path = "/test_dir/overwrite/test.csv"
    test_data = [
        ("foo", 1),
        ("bar", 2),
        ("baz", 3),
    ]
    test_schema = ["name", "value"]

    writer = SendJsonToSftp(
        # Parameters for the SFTPWriter part
        host=sftp_fixture.host,
        port=sftp_fixture.port,
        username="a",
        password="b",
        path=path,
        # Parameters for the PandasJsonBufferWriter part
        orient="split",
        columns=test_schema,
    )

    _df = spark.createDataFrame(test_data, test_schema)
    _expected_data = '{"columns":["name","value"],"index":[0,1,2],"data":[["foo",1],["bar",2],["baz",3]]}'

    # Act
    writer.write(_df)

    # Assert that the PandasCsvBufferWriter parameters are set correctly
    assert writer.buffer_writer.orient == "split"
    assert writer.buffer_writer.columns == test_schema
    assert writer.buffer_writer.compression is None

    # Assert that the data was written to the SFTP server
    actual_data = read_sftp_file(sftp_fixture, path)
    assert actual_data == _expected_data

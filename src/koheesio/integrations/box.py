"""
Box Module

The module is used to facilitate various interactions with Box service. The implementation is based on the
functionalities available in Box Python SDK: https://github.com/box/box-python-sdk

Prerequisites
-------------
* Box Application is created in the developer portal using the JWT auth method (Developer Portal - My Apps - Create)
* Application is authorized for the enterprise (Developer Portal - MyApp - Authorization)
"""

import re
from typing import Any, Dict, Optional, Union
from abc import ABC, abstractmethod
from datetime import datetime
from io import BytesIO
from pathlib import PurePath

from boxsdk import Client, JWTAuth
from boxsdk.object.file import File
from boxsdk.object.folder import Folder

from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, lit
from pyspark.sql.types import StructType

from koheesio import Step, StepOutput
from koheesio.models import (
    Field,
    SecretBytes,
    SecretStr,
    SkipValidation,
    conlist,
    field_validator,
    model_validator,
)
from koheesio.spark.readers import Reader


class BoxFolderNotFoundError(Exception):
    """Error when a provided box path does not exist."""


class BoxPathIsEmptyError(Exception):
    """Exception when provided Box path is empty or no files matched the mask."""


class Box(Step, ABC):
    """
    Configuration details required for the authentication can be obtained in the Box Developer Portal by generating
    the Public / Private key pair in "Application Name -> Configuration -> Add and Manage Public Keys".

    The downloaded JSON file will look like this:
    ```json
    {
      "boxAppSettings": {
        "clientID": "client_id",
        "clientSecret": "client_secret",
        "appAuth": {
          "publicKeyID": "public_key_id",
          "privateKey": "private_key",
          "passphrase": "pass_phrase"
        }
      },
      "enterpriseID": "123456"
    }
    ```
    This class is used as a base for the rest of Box integrations, however it can also be used separately to obtain
    the Box client which is created at class initialization.

    Examples
    --------
    ```python
    b = Box(
        client_id="client_id",
        client_secret="client_secret",
        enterprise_id="enterprise_id",
        jwt_key_id="jwt_key_id",
        rsa_private_key_data="rsa_private_key_data",
        rsa_private_key_passphrase="rsa_private_key_passphrase",
    )
    b.client
    ```
    """

    client_id: Union[SecretStr, SecretBytes] = Field(
        default=..., alias="clientID", description="Client ID from the Box Developer console."
    )
    client_secret: Union[SecretStr, SecretBytes] = Field(
        default=..., alias="clientSecret", description="Client Secret from the Box Developer console."
    )
    enterprise_id: Union[SecretStr, SecretBytes] = Field(
        default=..., alias="enterpriseID", description="Enterprise ID from the Box Developer console."
    )
    jwt_key_id: Union[SecretStr, SecretBytes] = Field(
        default=..., alias="publicKeyID", description="PublicKeyID for the public/private generated key pair."
    )
    rsa_private_key_data: Union[SecretStr, SecretBytes] = Field(
        default=..., alias="privateKey", description="Private key generated in the app management console."
    )
    rsa_private_key_passphrase: Union[SecretStr, SecretBytes] = Field(
        default=...,
        alias="passphrase",
        description="Private key passphrase generated in the app management console.",
    )

    client: SkipValidation[Client] = None

    def init_client(self):
        """Set up the Box client."""
        if not self.client:
            self.client = Client(JWTAuth(**self.auth_options))

    @property
    def auth_options(self):
        """
        Get a dictionary of authentication options, that can be handily used in the child classes
        """
        return {
            "client_id": self.client_id.get_secret_value(),
            "client_secret": self.client_secret.get_secret_value(),
            "enterprise_id": self.enterprise_id.get_secret_value(),
            "jwt_key_id": self.jwt_key_id.get_secret_value(),
            "rsa_private_key_data": self.rsa_private_key_data.get_secret_value().encode("utf-8"),
            "rsa_private_key_passphrase": self.rsa_private_key_passphrase.get_secret_value(),
        }

    def __init__(self, **data):
        super().__init__(**data)
        self.init_client()

    def execute(self):
        # Plug to be able to unit test ABC
        pass


class BoxFolderBase(Box):
    """
    Generic class to facilitate interactions with Box folders.

    Box SDK provides Folder class that has various properties and methods to interact with Box folders. The object can
    be obtained in multiple ways:
        * provide Box folder identified to `folder` parameter (the identifier can be obtained, for example, from URL)
        * provide existing object to `folder` parameter (boxsdk.object.folder.Folder)
        * provide filesystem-like path to `path` parameter

    See Also
    --------
    boxsdk.object.folder.Folder
    """

    folder: Optional[Union[Folder, str]] = Field(
        default=None, description="Existing folder object or folder identifier"
    )
    path: Optional[str] = Field(default=None, description="Path to the Box folder, for example: `folder/sub-folder/lz")

    root: Optional[Union[Folder, str]] = Field(
        default="0", description="Folder object or identifier of the folder that should be used as root"
    )

    class Output(StepOutput):
        """
        Define outputs for the BoxFolderBase class
        """

        folder: Optional[Folder] = Field(default=None, description="Box folder object")

    @model_validator(mode="after")
    def validate_folder_or_path(self):
        """
        Validations for 'folder' and 'path' parameter usage
        """
        folder_value = self.folder
        path_value = self.path

        if folder_value and path_value:
            raise AttributeError("Cannot user 'folder' and 'path' parameter at the same time")

        if not folder_value and not path_value:
            raise AttributeError("Neither 'folder' nor 'path' parameters are set")

        return self

    @property
    def _obj_from_id(self):
        """
        Get folder object from identifier
        """
        return self.client.folder(folder_id=self.folder).get() if isinstance(self.folder, str) else self.folder

    def action(self):
        """
        Placeholder for 'action' method, that should be implemented in the child classes

        Returns
        -------
            Folder or None
        """
        raise NotImplementedError

    def execute(self) -> Output:
        self.output.folder = self.action()


class BoxFolderGet(BoxFolderBase):
    """
    Get the Box folder object for an existing folder or create a new folder and parent directories.

    Examples
    --------
    ```python
    from koheesio.steps.integrations.box import BoxFolderGet

    auth_params = {...}
    folder = BoxFolderGet(**auth_params, path="/foo/bar").execute().folder
    # or
    folder = BoxFolderGet(**auth_params, path="1").execute().folder
    ```
    """

    create_sub_folders: Optional[bool] = Field(
        False, description="Create sub-folders recursively if the path does not exist."
    )

    def _get_or_create_folder(self, current_folder_object, next_folder_name):
        """
        Get or create a folder.

        Parameters
        ----------
        current_folder_object: Folder
            Current folder object.
        next_folder_name: str
            Name of the next folder.

        Returns
        -------
        next_folder_object: Folder
            Next folder object.
        """
        for item in current_folder_object.get_items():
            if item.type == "folder" and item.name == next_folder_name:
                return item

        if self.create_sub_folders:
            return current_folder_object.create_subfolder(name=next_folder_name)
        else:
            raise BoxFolderNotFoundError(
                f"The folder '{next_folder_name}' does not exist! Set 'create_sub_folders' to True "
                "to create required directory structure automatically."
            )

    def action(self):
        """
        Get folder action

        Returns
        -------
        folder: Folder
            Box Folder object as specified in Box SDK
        """
        current_folder_object = None

        if self.folder:
            current_folder_object = self._obj_from_id

        if self.path:
            cleaned_path_parts = [p for p in PurePath(self.path).parts if p.strip() not in [None, "", " ", "/"]]
            current_folder_object = self.client.folder(folder_id=self.root) if isinstance(self.root, str) else self.root

            for next_folder_name in cleaned_path_parts:
                current_folder_object = self._get_or_create_folder(current_folder_object, next_folder_name)

        self.log.info(f"Folder identified or created: {current_folder_object}")
        return current_folder_object


class BoxFolderCreate(BoxFolderGet):
    """
    Explicitly create the new Box folder object and parent directories.

    Examples
    --------
    ```python
    from koheesio.steps.integrations.box import BoxFolderCreate

    auth_params = {...}
    folder = BoxFolderCreate(**auth_params, path="/foo/bar").execute()
    ```
    """

    create_sub_folders: bool = Field(
        default=True, description="Create sub-folders recursively if the path does not exist."
    )

    @field_validator("folder")
    def validate_folder(cls, folder):
        """
        Validate 'folder' parameter
        """
        if folder:
            raise AttributeError("Only 'path' parameter is allowed in the context of folder creation.")


class BoxFolderDelete(BoxFolderBase):
    """
    Delete existing Box folder based on object, identifier or path.

    Examples
    --------
    ```python
    from koheesio.steps.integrations.box import BoxFolderDelete

    auth_params = {...}
    BoxFolderDelete(**auth_params, path="/foo/bar").execute()
    # or
    BoxFolderDelete(**auth_params, folder="1").execute()
    # or
    folder = BoxFolderGet(**auth_params, path="/foo/bar").execute().folder
    BoxFolderDelete(**auth_params, folder=folder).execute()
    ```
    """

    def action(self):
        """
        Delete folder action

        Returns
        -------
            None
        """
        if self.folder:
            folder = self._obj_from_id
        else:  # path
            folder = BoxFolderGet.from_step(self).action()

        self.log.info(f"Deleting Box folder '{folder}'...")
        folder.delete()


class BoxReaderBase(Box, Reader, ABC):
    """
    Base class for Box readers.
    """

    schema_: Optional[StructType] = Field(
        None,
        alias="schema",
        description="[Optional] Schema that will be applied during the creation of Spark DataFrame",
    )
    params: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="[Optional] Set of extra parameters that should be passed to the Spark reader.",
    )

    class Output(StepOutput):
        """Make default reader output optional to gracefully handle 'no-files / folder' cases."""

        df: Optional[DataFrame] = Field(default=None, description="The Spark DataFrame")

    @abstractmethod
    def execute(self) -> Output:
        raise NotImplementedError


class BoxCsvFileReader(BoxReaderBase):
    """
    Class facilitates reading one or multiple CSV files with the same structure directly from Box and
    producing Spark Dataframe.

    Notes
    -----
    To manually identify the ID of the file in Box, open the file through Web UI, and copy ID from
    the page URL, e.g. https://foo.ent.box.com/file/1234567890 , where 1234567890 is the ID.

    Examples
    --------
    ```python
    from koheesio.steps.integrations.box import BoxCsvFileReader
    from pyspark.sql.types import StructType

    schema = StructType(...)
    b = BoxCsvFileReader(
        client_id="",
        client_secret="",
        enterprise_id="",
        jwt_key_id="",
        rsa_private_key_data="",
        rsa_private_key_passphrase="",
        file=["1", "2"],
        schema=schema,
    ).execute()
    b.df.show()
    ```
    """

    file: Union[str, list[str]] = Field(default=..., description="ID or list of IDs for the files to read.")

    def execute(self):
        """
        Loop through the list of provided file identifiers and load data into dataframe.
        For traceability purposes the following columns will be added to the dataframe:
            * meta_file_id: the identifier of the file on Box
            * meta_file_name: name of the file

        Returns
        -------
        DataFrame
        """
        df = None
        for f in self.file:
            self.log.debug(f"Reading contents of file with the ID '{f}' into Spark DataFrame")
            file = self.client.file(file_id=f)
            data = file.content().decode("utf-8").splitlines()
            rdd = self.spark.sparkContext.parallelize(data)
            temp_df = self.spark.read.csv(rdd, header=True, schema=self.schema_, **self.params)
            temp_df = (
                temp_df
                # fmt: off
                .withColumn("meta_file_id", lit(file.object_id))
                .withColumn("meta_file_name", lit(file.get().name))
                .withColumn("meta_load_timestamp", expr("to_utc_timestamp(current_timestamp(), current_timezone())"))
                # fmt: on
            )

            df = temp_df if not df else df.union(temp_df)

        self.output.df = df


class BoxCsvPathReader(BoxReaderBase):
    """
    Read all CSV files from the specified path into the dataframe. Files can be filtered using the regular expression
    in the 'filter' parameter. The default behavior is to read all CSV / TXT files from the specified path.

    Notes
    -----
    The class does not contain archival capability as it is presumed that the user wants to make sure that the full
    pipeline is successful (for example, the source data was transformed and saved) prior to moving the source
    files. Use BoxToBoxFileMove class instead and provide the list of IDs from 'file_id' output.

    Examples
    --------
    ```python
    from koheesio.steps.integrations.box import BoxCsvPathReader

    auth_params = {...}
    b = BoxCsvPathReader(**auth_params, path="foo/bar/").execute()
    b.df  # Spark Dataframe
    ...  # do something with the dataframe
    from koheesio.steps.integrations.box import BoxToBoxFileMove

    bm = BoxToBoxFileMove(**auth_params, file=b.file_id, path="/foo/bar/archive")
    ```
    """

    path: str = Field(default=..., description="Box path")
    filter: Optional[str] = Field(default=r".csv|.txt$", description="[Optional] Regexp to filter folder contents")

    def execute(self):
        """
        Identify the list of files from the source Box path that match desired filter and load them into Dataframe
        """
        folder = BoxFolderGet.from_step(self).execute().folder

        # Identify the list of files that should be processed
        files = [item for item in folder.get_items() if item.type == "file" and re.search(self.filter, item.name)]

        if len(files) > 0:
            self.log.info(
                f"A total of {len(files)} files, that match the mask '{self.mask}' has been detected in {self.path}."
                f" They will be loaded into Spark Dataframe: {files}"
            )
        else:
            raise BoxPathIsEmptyError(f"Path '{self.path}' is empty or none of files match the mask '{self.filter}'")

        file = [file_id.object_id for file_id in files]
        self.output.df = BoxCsvFileReader.from_step(self, file=file).read()
        self.output.file = file  # e.g. if files should be archived after pipeline is successful


class BoxFileBase(Box):
    """
    Generic class to facilitate interactions with Box folders.

    Box SDK provides File class that has various properties and methods to interact with Box files. The object can
    be obtained in multiple ways:
        * provide Box file identified to `file` parameter (the identifier can be obtained, for example, from URL)
        * provide existing object to `file` parameter (boxsdk.object.file.File)

    Notes
    -----
    Refer to BoxFolderBase for mor info about `folder` and `path` parameters

    See Also
    --------
    boxsdk.object.file.File
    """

    files: conlist(Union[File, str], min_length=1) = Field(
        default=..., alias="file", description="List of Box file objects or identifiers"
    )

    folder: Optional[Union[Folder, str]] = Field(
        default=None, description="Existing folder object or folder identifier"
    )
    path: Optional[str] = Field(default=None, description="Path to the Box folder, for example: `folder/sub-folder/lz")

    def action(self, file: File, folder: Folder):
        """
        Abstract class for File level actions.
        """
        raise NotImplementedError

    def execute(self):
        """
        Generic execute method for all BoxToBox interactions. Deals with getting the correct folder and file objects
        from various parameter inputs
        """
        if self.path:
            _folder = BoxFolderGet.from_step(self).execute().folder
        else:
            _folder = self.client.folder(folder_id=self.folder) if isinstance(self.folder, str) else self.folder

        for _file in self.files:
            _file = self.client.file(file_id=_file) if isinstance(_file, str) else _file
            self.action(file=_file, folder=_folder)


class BoxToBoxFileCopy(BoxFileBase):
    """
    Copy one or multiple files to the target Box path.

    Examples
    --------
    ```python
    from koheesio.steps.integrations.box import BoxToBoxFileCopy

    auth_params = {...}
    BoxToBoxFileCopy(**auth_params, file=["1", "2"], path="/foo/bar").execute()
    # or
    BoxToBoxFileCopy(**auth_params, file=["1", "2"], folder="1").execute()
    # or
    folder = BoxFolderGet(**auth_params, path="/foo/bar").execute().folder
    BoxToBoxFileCopy(**auth_params, file=[File(), File()], folder=folder).execute()
    ```
    """

    def action(self, file: File, folder: Folder):
        """
        Copy file to the desired destination and extend file description with the processing info

        Parameters
        ----------
        file: File
            File object as specified in Box SDK
        folder: Folder
            Folder object as specified in Box SDK
        """
        self.log.info(f"Copying '{file.get()}' to '{folder.get()}'...")
        file.copy(parent_folder=folder).update_info(
            data={"description": "\n".join([f"File processed on {datetime.utcnow()}", file.get()["description"]])}
        )


class BoxToBoxFileMove(BoxFileBase):
    """
    Move one or multiple files to the target Box path

    Examples
    --------
    ```python
    from koheesio.steps.integrations.box import BoxToBoxFileMove

    auth_params = {...}
    BoxToBoxFileMove(**auth_params, file=["1", "2"], path="/foo/bar").execute()
    # or
    BoxToBoxFileMove(**auth_params, file=["1", "2"], folder="1").execute()
    # or
    folder = BoxFolderGet(**auth_params, path="/foo/bar").execute().folder
    BoxToBoxFileMove(**auth_params, file=[File(), File()], folder=folder).execute()
    ```
    """

    def action(self, file: File, folder: Folder):
        """
        Move file to the desired destination and extend file description with the processing info

        Parameters
        ----------
        file: File
            File object as specified in Box SDK
        folder: Folder
            Folder object as specified in Box SDK
        """
        self.log.info(f"Moving '{file.get()}' to '{folder.get()}'...")
        file.move(parent_folder=folder).update_info(
            data={"description": "\n".join([f"File processed on {datetime.utcnow()}", file.get()["description"]])}
        )


class BoxFileWriter(BoxFolderBase):
    """
    Write file or a file-like object to Box.

    Examples
    --------
    ```python
    from koheesio.steps.integrations.box import BoxFileWriter

    auth_params = {...}
    f1 = BoxFileWriter(**auth_params, path="/foo/bar", file="path/to/my/file.ext").execute()
    # or
    import io

    b = io.BytesIO(b"my-sample-data")
    f2 = BoxFileWriter(**auth_params, path="/foo/bar", file=b, name="file.ext").execute()
    ```
    """

    file: Union[str, BytesIO] = Field(default=..., description="Path to file or a file-like object")
    file_name: Optional[str] = Field(
        default=None,
        description="When file path or name is provided to 'file' parameter, this will override the original name."
        "When binary stream is provided, the 'name' should be used to set the desired name for the Box file.",
    )
    description: Optional[str] = Field(None, description="Optional description to add to the file in Box")

    class Output(StepOutput):
        """Output class for BoxFileWriter."""

        file: File = Field(default=..., description="File object in Box")
        shared_link: str = Field(default=..., description="Shared link for the Box file")

    @model_validator(mode="before")
    def validate_name_for_binary_data(cls, values):
        """Validate 'file_name' parameter when providing a binary input for 'file'."""
        file, file_name = values.get("file"), values.get("file_name")
        if not isinstance(file, str) and not file_name:
            raise AttributeError("The parameter 'file_name' is mandatory when providing a binary input for 'file'.")

        return values

    def action(self):
        _file = self.file
        _name = self.file_name

        if isinstance(_file, str):
            _name = _name if _name else PurePath(_file).name
            with open(_file, "rb") as f:
                _file = BytesIO(f.read())

        folder: Folder = BoxFolderGet.from_step(self, create_sub_folders=True).execute().folder
        folder.preflight_check(size=0, name=_name)

        self.log.info(f"Uploading file '{_name}' to Box folder '{folder.get().name}'...")
        _box_file: File = folder.upload_stream(file_stream=_file, file_name=_name, file_description=self.description)

        self.output.file = _box_file
        self.output.shared_link = _box_file.get_shared_link()

    def execute(self) -> Output:
        self.action()

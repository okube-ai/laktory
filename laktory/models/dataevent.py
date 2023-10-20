import os
import json
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any
from typing import Literal
from typing import Union

from pydantic import Field
from pydantic import ConfigDict

from laktory.models.dataeventheader import DataEventHeader
from laktory.models.producer import Producer
from laktory._settings import settings
from laktory._logger import get_logger

logger = get_logger(__name__)

EXCLUDES = [
    "events_root",
    "event_path",
    "tstamp_col",
]


class DataEvent(DataEventHeader):
    model_config = ConfigDict(populate_by_name=True)
    name: str = Field(..., alias="event_name")
    description: Union[str, None] = Field(None, alias="event_description")
    producer: Producer = Field(None, alias="event_producer")
    data: dict
    tstamp_col: str = "created_at"

    def model_post_init(self, __context):
        # Add metadata
        self.data["_name"] = self.name
        self.data["_producer_name"] = self.producer.name
        tstamp = self.data.get(self.tstamp_col, datetime.utcnow())
        if not tstamp.tzinfo:
            tstamp = tstamp.replace(tzinfo=ZoneInfo("UTC"))
        self.data["_created_at"] = tstamp

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def created_at(self) -> datetime:
        return self.data["_created_at"]

    # ----------------------------------------------------------------------- #
    # Paths                                                                   #
    # ----------------------------------------------------------------------- #

    @property
    def dirpath(self) -> str:
        t = self.created_at
        return f"{self.event_root}{t.year:04d}/{t.month:02d}/{t.day:02d}/"

    def get_filename(self, fmt="json", suffix=None) -> str:
        t = self.created_at
        const = {"mus": {"s": 1e-6}, "s": {"ms": 1000}}  # TODO: replace with constants
        total_ms = int(
            (t.second + t.microsecond * const["mus"]["s"]) * const["s"]["ms"]
        )
        time_str = f"{t.hour:02d}{t.minute:02d}{total_ms:05d}Z"
        prefix = self.name
        if suffix is not None:
            prefix += f"_{suffix}"
        if fmt == "json_stream":
            fmt = "txt"
        return f"{prefix}_{t.year:04d}{t.month:02d}{t.day:02d}T{time_str}.{fmt}"

    def get_landing_filepath(self, fmt="json", suffix=None):
        return os.path.join(self.dirpath, self.get_filename(fmt, suffix))

    def get_storage_filepath(self, fmt="json", suffix=None):
        path = self.get_landing_filepath(fmt=fmt, suffix=suffix)
        path = path.replace(settings.workspace_landing_root, "/")
        return path

    # ----------------------------------------------------------------------- #
    # Output                                                                  #
    # ----------------------------------------------------------------------- #

    def model_dump(self, *args, **kwargs) -> dict[str, Any]:
        kwargs["exclude"] = kwargs.pop("exclude", EXCLUDES)
        kwargs["by_alias"] = kwargs.pop("by_alias", True)
        kwargs["mode"] = kwargs.pop("mode", "json")
        return super().model_dump(*args, **kwargs)

    def model_dump_json(self, *args, **kwargs) -> str:
        kwargs["exclude"] = kwargs.pop("exclude", EXCLUDES)
        return super().model_dump_json(*args, **kwargs)

    def _overwrite_or_skip(
        self, f: callable, path: str, exists: bool, overwrite: bool, skip: bool
    ):
        if exists:
            if skip:
                logger.info(f"Event {self.name} ({path}) already exists. Skipping.")

            else:
                if overwrite:
                    logger.info(f"Writing event {self.name} to {path}")
                    f()
                else:
                    raise FileExistsError(f"Object {path} already exists.")

        else:
            logger.info(f"Writing event {self.name} to {path}")
            f()

    def to_azure_storage_container(
        self,
        suffix: str = None,
        fmt: str = "json",
        container_client: Any = None,
        account_url: str = None,
        container_name: str = "landing",
        overwrite: bool = False,
        skip_if_exists: bool = False,
    ) -> None:
        # Set container client
        if container_client is None:
            from azure.storage.blob import ContainerClient
            from azure.identity import DefaultAzureCredential

            if account_url:
                # From account URL
                # TODO: test
                container_client = ContainerClient(
                    account_url,
                    credential=DefaultAzureCredential(),
                    container_name=container_name,
                )

            elif settings.lakehouse_sa_conn_str is not None:
                # From connection string
                container_client = ContainerClient.from_connection_string(
                    conn_str=settings.lakehouse_sa_conn_str,
                    container_name=container_name,
                )

            else:
                raise ValueError(
                    "Provide a valid container client, an account url or a connection string in LAKEHOUSE_SA_CONN_STR"
                )

        path = self.get_storage_filepath(suffix=suffix, fmt=fmt)
        blob = container_client.get_blob_client(path)

        def _write():
            if fmt != "json":
                raise NotImplementedError()

            blob.upload_blob(
                self.model_dump_json(),
                overwrite=overwrite,
            )

        self._overwrite_or_skip(
            f=_write,
            path=path,
            exists=blob.exists(),
            overwrite=overwrite,
            skip=skip_if_exists,
        )

    def to_aws_s3_bucket(
        self,
        bucket_name,
        suffix: str = None,
        fmt: str = "json",
        s3_resource: Any = None,
        overwrite: bool = False,
        skip_if_exists: bool = False,
    ) -> None:
        import boto3

        if s3_resource is None:
            s3_resource = boto3.resource(
                service_name="s3",
                aws_access_key_id=settings.aws_access_key_id,
                aws_secret_access_key=settings.aws_secret_access_key,
                region_name=settings.aws_region,
            )

        bucket = s3_resource.Bucket(bucket_name)

        path = self.get_storage_filepath(suffix=suffix, fmt=fmt)[1:]

        object_exists = True
        try:
            o = bucket.Object(path)
            o.load()
        except Exception as e:
            if "404" in str(e):
                object_exists = False
            else:
                raise e

        def _write():
            if fmt != "json":
                raise NotImplementedError()

            bucket.put_object(
                Key=path,
                Body=self.model_dump_json(),
            )

        self._overwrite_or_skip(
            f=_write,
            path=path,
            exists=object_exists,
            overwrite=overwrite,
            skip=skip_if_exists,
        )

    def to_gcp_storage_bucket(self):
        raise NotImplementedError()

    def to_databricks(
        self,
        suffix: str = None,
        fmt: str = "json",
        overwrite: bool = False,
        skip_if_exists: bool = False,
        storage_type: Literal["VOLUME", "MOUNT"] = "VOLUME",
    ) -> None:
        path = self.get_landing_filepath(suffix=suffix, fmt=fmt)
        if storage_type == "MOUNT":
            path = "/dbfs" + path
        dirpath = os.path.dirname(path)

        if not os.path.exists(dirpath):
            os.makedirs(dirpath)

        def _write():
            if fmt != "json":
                raise NotImplementedError()

            with open(path, "w") as fp:
                fp.write(self.model_dump_json())

        self._overwrite_or_skip(
            f=_write,
            path=path,
            exists=os.path.exists(path),
            overwrite=overwrite,
            skip=skip_if_exists,
        )

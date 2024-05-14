import os
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any
from typing import Literal
from typing import Union

from pydantic import Field
from pydantic import ConfigDict

from laktory.models.basemodel import BaseModel
from laktory.models.dataproducer import DataProducer
from laktory._settings import settings
from laktory._logger import get_logger

logger = get_logger(__name__)

# Metadata to exclude
EXCLUDES = [
    "events_root_",
    "event_root_",
    "tstamp_col",
    "tstamp_in_path",
]


class DataEvent(BaseModel):
    """
    Data Event class defines both the context (metadata) describing a data
    event and its content. It is generally used to write data to a storage
    location.

    Attributes
    ----------
    data:
        Event data stored as a (key, value) object
    description:
        Data event description
    event_root:
        Root path for specific event. Default value: `{settings.workspace_landing_root}/events/my-event`
    events_root:
        Root path for all events. Default value: `{settings.workspace_landing_root}/events/`
    name:
        Data event name
    producer:
        Data event producer
    tstamp_col:
        Column storing event UTC timestamp
    tstamp_in_path:
        If `True`, includes timestamp if data event filepath

    Examples
    --------
    This is example one
    ```py
    from laktory import models
    from datetime import datetime

    event = models.DataEvent(
        name="stock_price",
        producer={"name": "yahoo-finance"},
    )
    print(event)
    '''
    variables={} data=None description=None events_root_=None event_root_=None name='stock_price' producer=DataProducer(variables={}, name='yahoo-finance', description=None, party=1) tstamp_col='created_at' tstamp_in_path=True
    '''

    print(event.event_root)
    #> /Volumes/dev/sources/landing/events/yahoo-finance/stock_price/

    event = models.DataEvent(
        name="stock_price",
        producer={"name": "yahoo-finance"},
        data={
            "created_at": datetime(2023, 8, 23),
            "symbol": "GOOGL",
            "open": 130.25,
            "close": 132.33,
        },
    )
    print(event)
    '''
    variables={} data={'created_at': datetime.datetime(2023, 8, 23, 0, 0), 'symbol': 'GOOGL', 'open': 130.25, 'close': 132.33, '_name': 'stock_price', '_producer_name': 'yahoo-finance', '_created_at': datetime.datetime(2023, 8, 23, 0, 0, tzinfo=zoneinfo.ZoneInfo(key='UTC'))} description=None events_root_=None event_root_=None name='stock_price' producer=DataProducer(variables={}, name='yahoo-finance', description=None, party=1) tstamp_col='created_at' tstamp_in_path=True
    '''

    print(event.dirpath)
    #> /Volumes/dev/sources/landing/events/yahoo-finance/stock_price/2023/08/23/

    print(event.get_landing_filepath())
    '''
    /Volumes/dev/sources/landing/events/yahoo-finance/stock_price/2023/08/23/stock_price_20230823T000000000Z.json
    '''

    print(event.get_storage_filepath())
    #> /events/yahoo-finance/stock_price/2023/08/23/stock_price_20230823T000000000Z.json
    ```
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)
    data: Union[dict, None] = None
    description: Union[str, None] = None
    events_root_: Union[str, None] = Field(None, alias="events_root")
    event_root_: Union[str, None] = Field(None, alias="event_root")
    name: str
    producer: DataProducer = None
    tstamp_col: str = "created_at"
    tstamp_in_path: bool = True

    def model_post_init(self, __context):
        super().model_post_init(__context)

        # Add metadata
        if self.data is not None:
            self.data["_name"] = self.name
            self.data["_producer_name"] = self.producer.name
            tstamp = self.data.get(self.tstamp_col)
            if isinstance(tstamp, str):
                tstamp = datetime.fromisoformat(tstamp)
            elif tstamp is None:
                tstamp = datetime.utcnow()
            if not tstamp.tzinfo:
                tstamp = tstamp.replace(tzinfo=ZoneInfo("UTC"))
            self.data["_created_at"] = tstamp

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def created_at(self) -> datetime:
        """Timestamp at which event was created."""
        return self.data["_created_at"]

    # ----------------------------------------------------------------------- #
    # Paths                                                                   #
    # ----------------------------------------------------------------------- #

    @property
    def events_root(self) -> str:
        """Must be computed to dynamically account for settings (env variable at run time)"""
        if self.events_root_:
            return self.events_root_
        return settings.workspace_landing_root + "events/"

    @property
    def event_root(self) -> str:
        """
        Root path for the event. Default path is `{self.events_roots}/{producer_name}/{event_name}/`, but it may
        be overwritten by self.event_root_.

        Returns
        -------
        str
            Event path
        """
        if self.event_root_:
            return self.event_root_

        producer = ""
        if self.producer is not None:
            producer = self.producer.name + "/"
        v = f"{self.events_root}{producer}{self.name}/"
        return v

    @property
    def dirpath(self) -> str:
        """Path of the directory storing the event data."""
        dirpath = self.event_root
        if self.tstamp_in_path:
            t = self.created_at
            dirpath += f"{t.year:04d}/{t.month:02d}/{t.day:02d}/"
        return dirpath

    def get_filename(self, fmt: str = "json", suffix: str = None) -> str:
        """
        Get file name associated with the data of the even, given a format `fmt`
        and a `suffix`.

        Parameters
        ----------
        fmt
            File format
        suffix
            File path suffix

        Returns
        -------
        str
            Data file path
        """
        filename = self.name
        if suffix is not None:
            filename += f"_{suffix}"
        if self.tstamp_in_path:
            t = self.created_at
            const = {
                "mus": {"s": 1e-6},
                "s": {"ms": 1000},
            }  # TODO: replace with constants
            total_ms = int(
                (t.second + t.microsecond * const["mus"]["s"]) * const["s"]["ms"]
            )
            time_str = f"{t.hour:02d}{t.minute:02d}{total_ms:05d}Z"
            filename += f"_{t.year:04d}{t.month:02d}{t.day:02d}T{time_str}"

        filename += f".{fmt}"

        return filename

    def get_landing_filepath(self, fmt="json", suffix=None):
        """
        Get file path on the landing mount/volume associated with the data of
        the event, given a format `fmt` and a `suffix`.

        Parameters
        ----------
        fmt
            File format
        suffix
            File path suffix

        Returns
        -------
        str
            Data file path
        """
        return os.path.join(self.dirpath, self.get_filename(fmt, suffix))

    def get_storage_filepath(self, fmt="json", suffix=None):
        """
        Get file path on the landing storage associated with the data of the
        event, given a format `fmt` and a `suffix`.

        Parameters
        ----------
        fmt
            File format
        suffix
            File path suffix

        Returns
        -------
        str
            Data file path
        """
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

    def to_path(
        self,
        suffix: str = None,
        fmt: str = "json",
        overwrite: bool = False,
        skip_if_exists: bool = False,
    ) -> None:
        """
        Write data event to local file path, given a format `fmt` and a
        `suffix`.

        Parameters
        ----------
        suffix:
            File path suffix
        fmt:
            File format
        overwrite:
            Overwrite file if already exists
        skip_if_exists:
            If `True` and file already exists, writing is skipped.
        """
        path = self.get_landing_filepath(suffix=suffix, fmt=fmt)
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
        """
        Write data event to azure storage container, given a format `fmt` and a
        `suffix`.

        Parameters
        ----------
        suffix:
            File path suffix
        fmt:
            File format
        container_client:
            Authorized Azure container client
        account_url:
            URL of storage account
        container_name:
            Name of the storage container
        overwrite:
            Overwrite file if already exists
        skip_if_exists:
            If `True` and file already exists, writing is skipped.
        """
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
        """
        Write data event to azure storage container, given a format `fmt` and a
        `suffix`.

        Parameters
        ----------
        suffix:
            File path suffix
        fmt:
            File format
        s3_resource:
            Authorized S3 bucket resource
        overwrite:
            Overwrite file if already exists
        skip_if_exists:
            If `True` and file already exists, writing is skipped.
        """
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
        storage_type: Union[Literal["VOLUME", "MOUNT"], str] = "VOLUME",
    ) -> None:
        """
        Write data event to Databricks volume or mount, given a format `fmt`
        and a `suffix`.

        Parameters
        ----------
        suffix:
            File path suffix
        fmt:
            File format
        overwrite:
            Overwrite file if already exists
        skip_if_exists:
            If `True` and file already exists, writing is skipped.
        storage_type:
            Specify if data is written to a mount or a volume.
        """
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


if __name__ == "__main__":
    from laktory import models

    event = models.DataEvent(
        name="stock_price",
        producer={"name": "yahoo-finance"},
        data={
            "created_at": datetime(2023, 8, 23),
            "symbol": "GOOGL",
            "open": 130.25,
            "close": 132.33,
        },
    )
    print(event)

    print(event.dirpath)
    print(event.get_landing_filepath())
    print(event.get_storage_filepath())

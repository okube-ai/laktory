import os
import json
from datetime import datetime

from azure.storage.blob import ContainerClient
from laktory.models import EventData

# --------------------------------------------------------------------------- #
# Container Client                                                            #
# --------------------------------------------------------------------------- #

container = ContainerClient.from_connection_string(
    conn_str=os.getenv("LAKTORY_SA_CONN_STR"),
    container_name="landing",
)


# --------------------------------------------------------------------------- #
# Fetch data                                                                  #
# --------------------------------------------------------------------------- #

event = EventData(
    name="stocks",
    producer={"name": "bank"},
    data={
        "created": datetime(2023, 7, 1),
        "symbol": "x",
        "value": 1,
    },
    landing_mount_path="",
)

blob = container.get_blob_client(
    blob=event.get_landing_filepath()
)


data = event.model_dump()

blob.upload_blob(json.dumps(data))

# json.dumps(data)
# print(cs)

# print(event)

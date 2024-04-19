import os
import requests

# --------------------------------------------------------------------------- #
# Setup                                                                       #
# --------------------------------------------------------------------------- #

host = os.getenv("DATABRICKS_HOST")
token = os.getenv("DATABRICKS_TOKEN")
url = f"{host}api/2.0/pipelines"
headers = {
    "Authorization": f"Bearer {token}",
    "User-Agent": "databricks-tf-provider/1.35.0 databricks-sdk-go/0.30.0 go/1.21.6 os/darwin terraform/1.6.6 resource/pipeline auth/pat okube/laktory",
}


# --------------------------------------------------------------------------- #
# Requests                                                                    #
# --------------------------------------------------------------------------- #

response = requests.get(url=url, headers=headers)

print(response.json())

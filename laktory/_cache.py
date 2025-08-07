from pathlib import Path

from platformdirs import user_cache_dir

app_name = "laktory"
author = "okube"

cache_dir = Path(user_cache_dir(app_name, author))

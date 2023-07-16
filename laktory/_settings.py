from settus import BaseSettings


class Settings(BaseSettings):
    landing_mount_path: str = "mnt/landing/"


settings = Settings()

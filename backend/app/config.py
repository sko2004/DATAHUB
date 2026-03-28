from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    database_url: str = "postgresql://datahub_admin:datahub_pass@localhost:5432/datahub_db"
    secret_key: str = "datahub_super_secret_key_2024_module5_secure"
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 60
    groq_api_key: str = ""
    upload_dir: str = "./uploads"

    class Config:
        env_file = ".env"


@lru_cache()
def get_settings() -> Settings:
    return Settings()

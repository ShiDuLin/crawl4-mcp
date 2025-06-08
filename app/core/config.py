
from pathlib import Path
from typing import Any
import os

from pydantic_settings import BaseSettings, SettingsConfigDict

APP_DIR = Path(__file__).parent.parent


def parse_cors(v: Any) -> list[str] | str:
    if isinstance(v, str) and not v.startswith("["):
        return [i.strip() for i in v.split(",")]
    elif isinstance(v, list | str):
        return v
    raise ValueError(v)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        # 验证默认值是否正确
        validate_default=False,
        # 优先级：后面文件的配置会覆盖前面文件的配置
        env_file=[".env"],
        env_ignore_empty=True,
        env_file_encoding="utf-8",
        # 忽略未定义的配置
        extra="ignore",
    )

    PROJECT_NAME: str

    # OpenAI API Key
    OPENAI_API_KEY: str
    OPENAI_BASE_URL: str = "https://api.openai.com/v1"
    OPENAI_MODEL: str = "gpt-4o-mini"

    # 分块配置
    CHUNK_SIZE: int = 5000
    CHUNK_OVERLAP: int = 50

    # Supabase Configuration
    SUPABASE_URL: str
    SUPABASE_SERVICE_KEY: str

    # USE_CONTEXTUAL_EMBEDDINGS: bool = False

    TRANSPORT: str = 'sse'

    MD_FILE_SAVE_BASE_DIR: str = os.path.join(os.getcwd(), "markdowns")


settings: Settings = Settings()  # type: ignore

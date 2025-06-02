from .config import settings
from .log_adapter import logger, setup_logging
from .db import get_supabase_client

__all__ = ["settings", "get_supabase_client", "logger"]

# 初始化日志库
setup_logging()
from supabase import Client, create_client
from app.core.config import settings


def get_supabase_client() -> Client:
    """
    获取Supabase客户端实例，使用环境变量中的URL和密钥。
    
    Returns:
        Supabase客户端实例
    """
    url = settings.SUPABASE_URL
    key = settings.SUPABASE_SERVICE_KEY
    
    if not url or not key:
        raise ValueError("SUPABASE_URL和SUPABASE_SERVICE_KEY必须在环境变量中设置")
    
    return create_client(url, key)
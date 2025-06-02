from urllib.parse import urlparse
import os
from app.core import settings


def save_to_md(url: str, md_content: str) -> str:
    """
    Save markdown content to a file, organized by domain in separate directories.
    For the same URL, it will overwrite the existing file rather than creating a new one.
    
    Args:
        url: URL of the crawled page
        md_content: Markdown content to save
        
    Returns:
        str: Path to the saved file
    """
    parsed_url = urlparse(url)
    domain = parsed_url.netloc
    if not domain:
        domain = "unknown_domain"
    
    # Create a stable, identifiable filename based on URL path and query
    path_part = parsed_url.path.replace("/", "_").strip("_")
    query_part = f"_{parsed_url.query}" if parsed_url.query else ""
    readable = f"{path_part}{query_part}" if path_part or query_part else "index"
    readable = readable[:100]  # Limit length but keep more URL path info
    
    # No timestamp in filename to allow overwriting same URL content
    file_name = f"{readable}.md"
    
    # Create domain-specific directory
    base_dir = settings.MD_FILE_SAVE_BASE_DIR
    domain_dir = os.path.join(base_dir, domain)
    os.makedirs(domain_dir, exist_ok=True)
    
    file_path = os.path.join(domain_dir, file_name)
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(md_content)
    
    return file_path
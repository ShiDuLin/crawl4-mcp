import asyncio
import json
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

from crawl4ai import AsyncWebCrawler, BrowserConfig, CacheMode, CrawlerRunConfig
from fastmcp import Context, FastMCP

from app.utils.crawl_util import (
    auto_get_sitemap_url,
    crawl_batch,
    crawl_markdown_file,
    crawl_recursive_internal_links,
    is_sitemap,
    is_txt,
    parse_sitemap,
)
from app.utils.file_util import save_to_md


@dataclass
class Crawl4AIContext:
    """Crawl MCP server 上下文"""

    crawler: AsyncWebCrawler


@asynccontextmanager
async def crawl4ai_lifespan(server: FastMCP) -> AsyncIterator[Crawl4AIContext]:
    """
    Manages the Crawl4AI client lifecycle.

    Args:
    server: FastMCP instance

    Yields:
    Crawl4AIContext: Contains the context of the Crawl4AI crawler and Supabase client
    """
    browser_config = BrowserConfig(headless=True, verbose=False)

    async with AsyncWebCrawler(config=browser_config) as crawler:
        yield Crawl4AIContext(
            crawler=crawler,
        )


mcp = FastMCP(
    "crawl4-mcp",
    description="MCP server for web crawling with Crawl4AI",
    lifespan=crawl4ai_lifespan,
    host=os.getenv("HOST", "0.0.0.0"),
    port=os.getenv("PORT", "8000"),
)


@mcp.tool()
async def crawl_single_page(ctx: Context, url: str) -> str:
    """
    Crawl a single web page and saving content as local markdown files.

    This tool is ideal for quickly retrieving content from a specific URL without following links.

    Args:
        ctx: The MCP server provided context
        url: URL of the web page to crawl

    Returns:
        JSON string with crawl summary including success status and file path
    """
    try:
        crawler = ctx.request_context.lifespan_context.crawler

        run_config = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,  # 跳过缓存读取，强制获取新鲜内容
            stream=False,  # 使用批处理模式而非流式处理
        )

        result = await crawler.arun(url=url, config=run_config)

        if result.success and result.markdown:
            file_path = save_to_md(url, result.markdown)
            return json.dumps(
                {"success": True, "url": url, "file_path": file_path}, indent=2
            )
        else:
            return json.dumps(
                {
                    "success": False,
                    "url": url,
                    "error": "No content found or crawl unsuccessful",
                },
                indent=2,
            )
    except Exception as e:
        ctx.logger.error(f"Error crawling {url}: {e}")
        return json.dumps({"success": False, "url": url, "error": str(e)}, indent=2)


@mcp.tool()
async def smart_crawl_with_auto_sitemap(
    ctx: Context,
    url: str,
    max_depth: int = 3,
    max_concurrent: int = 10,
) -> str:
    """
    Intelligently crawl a URL with automatic sitemap detection, saving content as local markdown files.

    This tool employs three crawling strategies:
    - For text files (e.g., llms.txt): Directly retrieves the content
    - For sitemaps: Parses and crawls all URLs in parallel
    - For regular webpages: First attempts to automatically discover the site's sitemap for batch crawling,
      and if no sitemap is found, use recursive crawling

    All crawled content is saved as local markdown files, accessible via the returned file_paths.

    Args:
        ctx: MCP server context
        url: URL to crawl (can be a regular webpage, sitemap.xml, or text file)
        max_depth: Maximum recursion depth for recursive crawling (default: 3)
        max_concurrent: Maximum number of concurrent browser sessions (default: 10)

    Returns:
        JSON string with crawl summary including crawl_type (text_file/sitemap/auto_sitemap/webpage) and file paths
    """
    try:
        crawler = ctx.request_context.lifespan_context.crawler
        # 1. txt/sitemap直接走原逻辑
        if is_txt(url):
            crawl_results = await crawl_markdown_file(crawler, url)
            crawl_type = "text_file"
        elif is_sitemap(url):
            sitemap_urls = parse_sitemap(url)
            if not sitemap_urls:
                return json.dumps(
                    {"success": False, "url": url, "error": "No URLs found in sitemap"},
                    indent=2,
                )
            crawl_results = await crawl_batch(
                crawler, sitemap_urls, max_concurrent=max_concurrent
            )
            crawl_type = "sitemap"
        else:
            # 2. 自动发现sitemap
            found_sitemap = await auto_get_sitemap_url(url)
            sitemap_urls = None
            if found_sitemap:
                sitemap_urls = parse_sitemap(found_sitemap)
                # 调试代码，限制url数量
                # sitemap_urls = sitemap_urls[:2]
            if found_sitemap and sitemap_urls:
                crawl_results = await crawl_batch(
                    crawler, sitemap_urls, max_concurrent=max_concurrent
                )
                crawl_type = "auto_sitemap"
            else:
                # 3. 递归爬取
                crawl_results = await crawl_recursive_internal_links(
                    crawler,
                    [url],
                    max_depth=max_depth,
                    max_concurrent=max_concurrent,
                )
                crawl_type = "webpage"

        if not crawl_results:
            return json.dumps(
                {"success": False, "url": url, "error": "No content found"}, indent=2
            )
        else:
            file_paths = []
            for result in crawl_results:
                if isinstance(result, dict) and result.get("markdown"):
                    file_path = save_to_md(result.get("url", url), result["markdown"])
                    file_paths.append(file_path)

        return json.dumps(
            {
                "success": True,
                "url": url,
                "crawl_type": crawl_type,
                "file_paths": file_paths,
            },
            indent=2,
        )
    except Exception as e:
        return json.dumps({"success": False, "url": url, "error": str(e)}, indent=2)


async def main():
    transport = os.getenv("TRANSPORT", "sse")
    if transport == "sse":
        # Run the MCP server with sse transport
        await mcp.run_sse_async()
    else:
        # Run the MCP server with stdio transport
        await mcp.run_stdio_async()


if __name__ == "__main__":
    asyncio.run(main())

# 配置LLM总结策略
# llm_config = LLMConfig(
#     provider=settings.OPENAI_MODEL, api_token=settings.OPENAI_API_KEY
# )
# run_config = CrawlerRunConfig(
#             cache_mode=CacheMode.BYPASS,  # 跳过缓存读取，强制获取新鲜内容
#             stream=False,  # 使用批处理模式而非流式处理
#             # extraction_strategy=summary_strategy,
#             # word_count_threshold=20,
#             # exclude_external_links=True,
#         )
# summary_strategy = LLMExtractionStrategy(
#     llm_config=llm_config,
#     instruction="""
#     Please provide a concise summary of the content of this page, including:
#     1. Main topics and key ideas
#     2. Key information points
#     3. Important conclusions or recommendations
#     Please keep it brief and clear.
#     """,
#     extraction_type="block",
# )

# if result.success and result.markdown:
#     parsed_url = urlparse(url)
#     source_id = parsed_url.netloc or parsed_url.path

#     chunks = smart_chunk_markdown(result.markdown)

#     urls = []
#     chunk_numbers = []
#     contents = []
#     meta_datas = []
#     total_word_count = 0

#     for i, chunk in enumerate(chunks):
#         urls.append(url)
#         chunk_numbers.append(i)
#         contents.append(chunk)
#         # 获取元数据
#         meta = extract_section_info(chunk)
#         meta["chunk_index"] = i
#         meta["url"] = url
#         meta["source"] = source_id
#         meta["crawl_time"] = str(asyncio.current_task().get_coro().__name__)
#         meta_datas.append(meta)

#         total_word_count += meta.get("word_count", 0)

#     # 获取LLM总结内容
#     summary = None
#     if hasattr(result, "extracted_content") and result.extracted_content:
#         # LLMExtractionStrategy返回的通常是一个list[dict]，取第一个block的content
#         if (
#             isinstance(result.extracted_content, list)
#             and result.extracted_content
#         ):
#             summary = result.extracted_content[0].get("content")
#         elif isinstance(result.extracted_content, str):
#             summary = result.extracted_content

#     # 存入sources表
#     if summary:
#         crud.update_source_info(
#             supabase_client, source_id, summary, total_word_count
#         )

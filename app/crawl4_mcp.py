import asyncio
import concurrent.futures
import json
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from urllib.parse import urlparse

from crawl4ai import AsyncWebCrawler, BrowserConfig, CacheMode, CrawlerRunConfig
from fastmcp import Context, FastMCP
from supabase import Client

from app.core.db import get_supabase_client
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
from app.utils.rag_util import (
    add_documents_to_supabase,
    chunk_by_headers,
    extract_source_summary,
    update_source_info,
)
from app.core import logger

@dataclass
class Crawl4AIContext:
    """Crawl MCP server 上下文"""

    crawler: AsyncWebCrawler
    supabase_client: Client


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
    supabase_client = get_supabase_client()
    

    async with AsyncWebCrawler(config=browser_config) as crawler:
        yield Crawl4AIContext(
            crawler=crawler,
            supabase_client=supabase_client,
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
        parsed_url = urlparse(url)
        source_id = parsed_url.netloc or parsed_url.path
        
        crawler = ctx.request_context.lifespan_context.crawler
        supabase_client = ctx.request_context.lifespan_context.supabase_client

        run_config = CrawlerRunConfig( 
            cache_mode=CacheMode.BYPASS,  # 跳过缓存读取，强制获取新鲜内容
            stream=False,  # 使用批处理模式而非流式处理
        )

        result = await crawler.arun(url=url, config=run_config)

        chunks = chunk_by_headers(result.markdown)
        # # 读取Markdown文件内容
        # with open(r'D:\work\my_note\项目\crawl4-mcp\zh_introduction_examples.md', 'r', encoding='utf-8') as f:
        #     markdown_content = f.read()
        # chunks = chunk_by_headers(markdown_content)

        urls = []
        chunk_numbers = []
        contents = []
        metadatas = []
        total_word_count = 0
        for i, chunk in enumerate(chunks):
            # with open(rf'D:\work\my_note\项目\crawl4-mcp\chunk_{i}.md', "w", encoding="utf-8") as f:
            #     f.write(chunk.page_content)
            chunk.metadata["source"] = source_id
            chunk.metadata["url"] = url
            chunk.metadata["chunk_index"] = i
            chunk.metadata["crawl_time"] = str(asyncio.current_task().get_coro().__name__)
            chunk.metadata["word_count"] = len(chunk.page_content.split())


            urls.append(url)
            chunk_numbers.append(i)
            contents.append(chunk.page_content)
            metadatas.append(chunk.metadata)
            total_word_count += chunk.metadata.get("word_count", 0)

        source_summary = extract_source_summary(source_id, result.markdown[:5000])
        if source_summary:
            update_source_info(supabase_client, source_id, source_summary, total_word_count)

        await add_documents_to_supabase(
            supabase_client,
            urls,
            chunk_numbers,
            contents,
            metadatas,
        )
        if result.success and result.markdown:
            save_to_md(url, result.markdown)
            return json.dumps(
                {"success": True, "url": url}, indent=2
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
        # ctx.logger.error(f"Error crawling {url}: {e}")
        logger.error(f"Error crawling {url}: {e}")
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

    All crawled content is saved as local markdown files.

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
        supabase_client = ctx.request_context.lifespan_context.supabase_client
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
            found_sitemap = await auto_get_sitemap_url(url)
            sitemap_urls = None
            if found_sitemap:
                sitemap_urls = parse_sitemap(found_sitemap)
            if found_sitemap and sitemap_urls:
                crawl_results = await crawl_batch(
                    crawler, sitemap_urls, max_concurrent=max_concurrent
                )
                crawl_type = "auto_sitemap"
            else:
                crawl_results = await crawl_recursive_internal_links(
                    crawler,
                    [url],
                    max_depth=max_depth,
                    max_concurrent=max_concurrent,
                )
                crawl_type = "webpage"

        if crawl_results:
            urls = []
            chunk_numbers = []
            contents = []
            metadatas = []
            chunk_count = 0

            source_content_map = {}
            source_word_counts = {}

            for result in crawl_results:
                if isinstance(result, dict) and result.get("markdown"):
                    save_to_md(result.get("url", url), result["markdown"])
                
                source_url = result['url']
                md = result['markdown']
                chunks = chunk_by_headers(md)

                parsed_url = urlparse(source_url)
                source_id = parsed_url.netloc or parsed_url.path

                if source_id not in source_content_map:
                    source_content_map[source_id] = md[:5000]  # Store first 5000 chars
                    source_word_counts[source_id] = 0

                for i, chunk in enumerate(chunks):
                    chunk.metadata["source"] = source_id
                    chunk.metadata["url"] = source_url
                    chunk.metadata["chunk_index"] = i
                    chunk.metadata["crawl_time"] = str(asyncio.current_task().get_coro().__name__)
                    chunk.metadata["crawl_type"] = crawl_type
                    chunk.metadata["word_count"] = len(chunk.page_content.split())


                    urls.append(source_url)
                    chunk_numbers.append(i)
                    contents.append(chunk.page_content)
                    metadatas.append(chunk.metadata)
                    source_word_counts[source_id] += chunk.metadata.get("word_count", 0)
                    chunk_count += 1

            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                source_summary_args = [(source_id, content) for source_id, content in source_content_map.items()]
                source_summaries = list(executor.map(lambda args: extract_source_summary(args[0], args[1]), source_summary_args))

            for source_id, summary in zip(source_content_map.keys(), source_summaries):
                update_source_info(supabase_client, source_id, summary, source_word_counts.get(source_id, 0))

            await add_documents_to_supabase(
                supabase_client,
                urls,
                chunk_numbers,
                contents,
                metadatas,
            )

        else:
            return json.dumps(
                {"success": False, "url": url, "error": "No content found"}, indent=2
            )
            

        return json.dumps(
            {
                "success": True,
                "url": url,
                "crawl_type": crawl_type,
                "pages_crawled": len(crawl_results),
                "sources_updated": len(source_content_map),
                "chunks_stored": chunk_count,
                "urls_crawled": [doc['url'] for doc in crawl_results][:5] + (["..."] if len(crawl_results) > 5 else [])
            },
            indent=2,
        )
    except Exception as e:
        logger.error(f"Error crawling {url}: {e}")
        return json.dumps({"success": False, "url": url, "error": str(e)}, indent=2)


@mcp.tool(
        description="Health check tool to verify whether the crawl4-mcp server is alive",
)
async def alive():
    return json.dumps(
                {
                    "success": True,
                    "msg": 'crawl4-mcp server is alive',
                },
                indent=2,
            )


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


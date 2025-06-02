import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock

import app.crawl4_mcp as crawl4_mcp

@pytest.mark.asyncio
async def test_crawl_single_page_success():
    # 构造 mock context
    mock_crawler = AsyncMock()
    mock_crawler.arun.return_value = "mocked result"
    mock_supabase_client = MagicMock()
    mock_logger = MagicMock()

    # 构造 lifespan_context
    class LifespanContext:
        crawler = mock_crawler
        supabase_client = mock_supabase_client

    # 构造 request_context
    class RequestContext:
        lifespan_context = LifespanContext()

    # 构造 ctx
    class Ctx:
        request_context = RequestContext()
        logger = mock_logger

    ctx = Ctx()
    url = "https://example.com"

    result = await crawl4_mcp.crawl_single_page(ctx, url)
    assert result == "mocked result"
    mock_crawler.arun.assert_awaited_once_with(url=url, config=pytest.ANY)

@pytest.mark.asyncio
async def test_crawl_single_page_exception():
    # 构造 mock context
    mock_crawler = AsyncMock()
    mock_crawler.arun.side_effect = Exception("crawl error")
    mock_supabase_client = MagicMock()
    mock_logger = MagicMock()

    class LifespanContext:
        crawler = mock_crawler
        supabase_client = mock_supabase_client

    class RequestContext:
        lifespan_context = LifespanContext()

    class Ctx:
        request_context = RequestContext()
        logger = mock_logger

    ctx = Ctx()
    url = "https://example.com"

    result = await crawl4_mcp.crawl_single_page(ctx, url)
    assert result is None
    mock_logger.error.assert_called_once()


if __name__ == "__main__":
    asyncio.run(test_crawl_single_page_success())
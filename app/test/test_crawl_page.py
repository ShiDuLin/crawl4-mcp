import asyncio
import app.crawl4_mcp as crawl4_mcp

class DummyLifespanContext:
    def __init__(self, crawler):
        self.crawler = crawler
        # self.supabase_client = supabase_client

class DummyRequestContext:
    def __init__(self, lifespan_context):
        self.lifespan_context = lifespan_context

class DummyCtx:
    def __init__(self, crawler, logger=None):
        self.request_context = DummyRequestContext(DummyLifespanContext(crawler))
        self.logger = logger

async def main():
    # 真实初始化 crawler 和 supabase_client
    # 这里直接用 app.mcp_server.crawl4ai_lifespan 生成上下文
    async with crawl4_mcp.crawl4ai_lifespan(None) as real_ctx:
        # real_ctx.crawler, real_ctx.supabase_client
        ctx = DummyCtx(real_ctx.crawler)
        # url = "https://www.promptingguide.ai/zh"
        url = "https://docs.crawl4ai.com/"
        # result = await mcp_server.crawl_single_page(ctx, url)
        result = await crawl4_mcp.smart_crawl_with_auto_sitemap(ctx, url)
        print("爬取结果：", result)

if __name__ == "__main__":
    asyncio.run(main())
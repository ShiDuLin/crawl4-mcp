# crawl4-mcp

本项目是一个crawl4ai 的爬虫MCP SERVER，提供高级网络爬虫。使用此 MCP SERVER，您可以抓取任何内容，将内容保存为本地markdown文件，然后在任何地方将该知识用于 RAG。

## 环境要求
- Python 3.12 或更高版本
- uv 包管理器

## 安装步骤

1. 克隆仓库
   ```bash
   git clone <仓库地址>
   cd crawl4-mcp
   ```

2. 使用 uv 创建虚拟环境并安装依赖
   ```bash
   uv venv -p 3.12
   .\.venv\Scripts\activate
   uv sync
   crawl4ai-setup
   ```
3. 根据下面的配置部分创建一个.env文件

## 运行
    ```bash
    uv run src/crawl4ai_mcp.py
    ```
server将启动并监听配置的主机和端口。

## 与 MCP client集成
一旦服务器使用 SSE 传输运行，您就可以使用以下配置连接到它：
```json
{
  "mcpServers": {
    "crawl4-mcp": {
      "transport": "sse",
      "url": "http://localhost:8051/sse"
    }
  }
}
```

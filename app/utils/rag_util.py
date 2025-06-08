import asyncio
import concurrent.futures
from typing import Any
from urllib.parse import urlparse

import openai
from langchain_text_splitters import (
    MarkdownHeaderTextSplitter,
    RecursiveCharacterTextSplitter,
)
from supabase import Client

from app.core import logger
from app.core.config import settings


# ===============================================
# 分块
# ===============================================
def chunk_by_headers(
    text: str, chunk_size: int = 5000, chunk_overlap: int = 200
) -> list[dict]:
    headers_to_split_on = [
        ("#", "H1"),
        ("##", "H2"),
        ("###", "H3"),
        ("####", "H4"),
        ("#####", "H5"),
        ("######", "H6"),
    ]
    markdown_splitter = MarkdownHeaderTextSplitter(
        headers_to_split_on, strip_headers=False
    )
    md_header_splits = markdown_splitter.split_text(text)

    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size, chunk_overlap=chunk_overlap
    )

    chunks = text_splitter.split_documents(md_header_splits)

    return chunks


# ===============================================
# 提取源摘要
# ===============================================
def extract_source_summary(source_id: str, content: str, max_length: int = 500) -> str:
    default_summary = f"Content from {source_id}"

    if not content or len(content.strip()) == 0:
        return default_summary

    truncated_content = content[:25000] if len(content) > 25000 else content

    prompt = f"""<source_content>
{truncated_content}
</source_content>

The above content is from the documentation for '{source_id}'. Please provide a concise summary (3-5 sentences) that describes what this library/tool/framework is about. The summary should help understand what the library/tool/framework accomplishes and the purpose.
"""

    try:
        response = openai.chat.completions.create(
            model=settings.OPENAI_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": "You are a helpful assistant that provides concise library/tool/framework summaries.",
                },
                {"role": "user", "content": prompt},
            ],
            temperature=0.3,
            max_tokens=150,
        )

        summary = response.choices[0].message.content.strip()

        if len(summary) > max_length:
            summary = summary[:max_length] + "..."

        return summary

    except Exception as e:
        logger.error(
            f"Error generating summary with LLM for {source_id}: {e}. Using default summary."
        )
        return default_summary


# ===============================================
# 批量Embedding
# ===============================================
def embed_text_batch(texts: list[str]) -> list[list[float]]:
    try:
        response = openai.embeddings.create(model="text-embedding-3-small", input=texts)
        return [item.embedding for item in response.data]
    except Exception as e:
        logger.error(f"failed to create embeddings: {e}")
        # 返回空列表，外层会处理
        return [None] * len(texts)


# ===============================================
# 单个Embedding
# ===============================================
def embed_text(text: str) -> list[float]:
    embeddings = embed_text_batch([text])
    return embeddings[0] if embeddings else [0.0] * 1536


# ===============================================
# 更新源信息
# ===============================================
def update_source_info(client: Client, source_id: str, summary: str, word_count: int):
    try:
        result = (
            client.table("sources")
            .update(
                {
                    "summary": summary,
                    "total_word_count": word_count,
                    "updated_at": "now()",
                }
            )
            .eq("source_id", source_id)
            .execute()
        )

        if not result.data:
            client.table("sources").insert(
                {
                    "source_id": source_id,
                    "summary": summary,
                    "total_word_count": word_count,
                }
            ).execute()
            logger.info(f"Created new source: {source_id}")
        else:
            logger.info(f"Updated source: {source_id}")

    except Exception as e:
        logger.error(f"Error updating source {source_id}: {e}")


async def insert_batch_with_retry(
    client, table, data, batch_idx, total_batches, max_retries=3
):
    for attempt in range(max_retries):
        try:
            result = (
                client.table(table)
                .upsert(
                    data,
                    on_conflict="url,chunk_number",
                )
                .execute()
            )

            logger.info(
                f"finished batch insert {batch_idx + 1}/{total_batches} ({len(data)} items)"
            )
            return result
        except Exception as e:
            logger.error(
                f"failed batch insert {batch_idx + 1} (attempt {attempt + 1}/{max_retries}): {e}"
            )

            if attempt == max_retries - 1:
                logger.info(f"skipped batch {batch_idx + 1} because of max retries")
                return None

            # 指数退避策略
            await asyncio.sleep(2**attempt)


# ===============================================
# 添加文档到Supabase
# ===============================================
async def add_documents_to_supabase(
    client: Client,
    urls: list[str],
    chunk_numbers: list[int],
    contents: list[str],
    metadatas: list[dict[str, Any]],
    batch_size: int = 30,
    max_workers: int = 8,
) -> None:
    # unique_urls = list(set(urls))

    # try:
    #     if unique_urls:
    #         client.table("crawled_pages").delete().in_("url", unique_urls).execute()
    # except Exception as e:
    #     logger.error(f"Batch delete failed: {e}. Trying one-by-one deletion as fallback.")
    #     for url in unique_urls:
    #         try:
    #             client.table("crawled_pages").delete().eq("url", url).execute()
    #         except Exception as inner_e:
    #             logger.error(f"Error deleting record for URL {url}: {inner_e}")

    # 1. 批量创建embedding (这是最耗时的部分，并行处理)
    total = len(urls)
    logger.info(f"start to process {total} items...")

    batched_contents = [
        contents[i : i + batch_size] for i in range(0, len(contents), batch_size)
    ]

    all_embeddings = []

    # 使用线程池并行处理embedding生成
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有批次的embedding任务
        future_to_batch = {
            executor.submit(embed_text_batch, batch): i
            for i, batch in enumerate(batched_contents)
        }

        # 处理完成的结果
        for future in concurrent.futures.as_completed(future_to_batch):
            batch_index = future_to_batch[future]
            try:
                embeddings = future.result()
                all_embeddings.extend(embeddings)
                logger.info(
                    f"finished batch embedding {batch_index + 1}/{len(batched_contents)}"
                )
            except Exception as e:
                logger.error(f"failed batch embedding {batch_index + 1}: {e}")

    # 2. 批量插入数据库
    logger.info("start to insert data to supabase...")
    # 准备所有批次的数据
    all_batches = []
    for i in range(0, total, batch_size):
        batch_data = []
        end_idx = min(i + batch_size, total)

        for j in range(i, end_idx):
            if j >= len(all_embeddings):
                continue  # 跳过没有成功生成embedding的项

            parsed_url = urlparse(urls[j])
            source_id = parsed_url.netloc or parsed_url.path
            data = {
                "url": urls[j],
                "chunk_number": chunk_numbers[j],
                "content": contents[j],
                "metadata": {"chunk_size": len(contents[j]), **(metadatas[j] or {})},
                "source_id": source_id,
                "embedding": all_embeddings[j],
                "created_at": "now()",  # 使用数据库时间函数
            }
            batch_data.append(data)

        if batch_data:
            all_batches.append(batch_data)

    # 使用异步处理并发插入批次
    insertion_tasks = []

    for batch_idx, batch in enumerate(all_batches):
        # 为每个批次创建一个异步任务
        task = asyncio.create_task(
            insert_batch_with_retry(
                client, "crawled_pages", batch, batch_idx, len(all_batches)
            )
        )
        insertion_tasks.append(task)

    # 等待所有插入任务完成
    await asyncio.gather(*insertion_tasks)

    logger.info(f"finished inserting {total} items")

from supabase import Client


def batch_delete_crawled_pages_by_urls(client: Client, urls: list[str]):
    # Get unique URLs to delete existing records
    unique_urls = list(set(urls))
    # Delete existing records for these URLs in a single operation
    try:
        if unique_urls:
            # Use the .in_() filter to delete all records with matching URLs
            client.table("crawled_pages").delete().in_("url", unique_urls).execute()
    except Exception as e:
        print(f"Batch delete failed: {e}. Trying one-by-one deletion as fallback.")
        # Fallback: delete records one by one
        for url in unique_urls:
            try:
                client.table("crawled_pages").delete().eq("url", url).execute()
            except Exception as inner_e:
                print(f"Error deleting record for URL {url}: {inner_e}")
                # Continue with the next URL even if one fails

def add_documents_to_supabase(
    client: Client,
    urls: list[str],
    chunk_numbers: list[int],
    contents: list[str],
    metadatas: list[dict],
    url_to_full_document: dict[str, str],
    embeddings: list = None,
    batch_size: int = 20
) -> None:
    """
    批量将文档分块插入到Supabase的crawled_pages表。
    插入前会先删除相同url的旧记录，防止重复。
    支持embedding、元数据、异常兜底。
    Args:
        client: Supabase client
        urls: 分块对应的url列表
        chunk_numbers: 分块编号
        contents: 分块内容
        metadatas: 分块元数据
        url_to_full_document: url到全文内容的映射（可用于上下文embedding）
        embeddings: 每个分块的embedding向量（如有）
        batch_size: 批量写入大小
    """
    import time
    from urllib.parse import urlparse

    # 1. 批量去重删除旧数据
    unique_urls = list(set(urls))
    try:
        if unique_urls:
            client.table("crawled_pages").delete().in_("url", unique_urls).execute()
    except Exception as e:
        print(f"Batch delete failed: {e}. Trying one-by-one deletion as fallback.")
        for url in unique_urls:
            try:
                client.table("crawled_pages").delete().eq("url", url).execute()
            except Exception as inner_e:
                print(f"Error deleting record for URL {url}: {inner_e}")

    # 2. 分批插入
    for i in range(0, len(contents), batch_size):
        batch_end = min(i + batch_size, len(contents))
        batch_urls = urls[i:batch_end]
        batch_chunk_numbers = chunk_numbers[i:batch_end]
        batch_contents = contents[i:batch_end]
        batch_metadatas = metadatas[i:batch_end]
        batch_embeddings = embeddings[i:batch_end] if embeddings is not None else [None] * len(batch_contents)

        batch_data = []
        for j in range(len(batch_contents)):
            parsed_url = urlparse(batch_urls[j])
            source_id = parsed_url.netloc or parsed_url.path
            data = {
                "url": batch_urls[j],
                "chunk_number": batch_chunk_numbers[j],
                "content": batch_contents[j],
                "metadata": batch_metadatas[j],
                "source_id": source_id,
            }
            if batch_embeddings[j] is not None:
                data["embedding"] = batch_embeddings[j]
            batch_data.append(data)

        # 3. 批量插入，失败兜底单条插入
        max_retries = 3
        retry_delay = 1.0
        for retry in range(max_retries):
            try:
                client.table("crawled_pages").insert(batch_data).execute()
                break
            except Exception as e:
                if retry < max_retries - 1:
                    print(f"Error inserting batch (attempt {retry + 1}/{max_retries}): {e}")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    print(f"Failed to insert batch after {max_retries} attempts: {e}")
                    print("Attempting to insert records individually...")
                    for record in batch_data:
                        try:
                            client.table("crawled_pages").insert(record).execute()
                        except Exception as individual_error:
                            print(f"Failed to insert individual record for URL {record['url']}: {individual_error}")
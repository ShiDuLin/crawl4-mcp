from typing import Any
import re

def smart_chunk_markdown(text: str, chunk_size: int = 5000) -> list[str]:
    """
    Split text into chunks, respecting code blocks, paragraphs, and both English and Chinese sentence boundaries.
    """
    chunks = []
    start = 0
    text_length = len(text)

    # 中文句子分隔符
    cn_sentence_seps = ['。', '！', '？']
    # 英文句子分隔符
    en_sentence_sep = '. '

    while start < text_length:
        end = start + chunk_size

        if end >= text_length:
            chunks.append(text[start:].strip())
            break

        chunk = text[start:end]
        code_block = chunk.rfind('```')
        if code_block != -1 and code_block > chunk_size * 0.3:
            end = start + code_block
        elif '\n\n' in chunk:
            last_break = chunk.rfind('\n\n')
            if last_break > chunk_size * 0.3:
                end = start + last_break
        else:
            # 查找最后一个中文句子分隔符
            last_cn_sep = -1
            for sep in cn_sentence_seps:
                idx = chunk.rfind(sep)
                if idx > last_cn_sep:
                    last_cn_sep = idx
            # 查找最后一个英文句子分隔符
            last_en_sep = chunk.rfind(en_sentence_sep)
            # 选择更靠后的分隔符
            last_sep = max(last_cn_sep, last_en_sep)
            if last_sep > chunk_size * 0.3:
                # 中文分隔符不包含空格，英文分隔符包含空格
                if last_sep == last_en_sep:
                    end = start + last_en_sep + len(en_sentence_sep)
                else:
                    end = start + last_cn_sep + 1

        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        start = end

    return chunks


def extract_section_info(chunk: str) -> dict[str, Any]:
    """
    Extracts headers and stats from a chunk.
    
    Args:
        chunk: Markdown chunk
        
    Returns:
        Dictionary with headers and stats
    """
    headers = re.findall(r'^(#+)\s+(.+)$', chunk, re.MULTILINE)
    header_str = '; '.join([f'{h[0]} {h[1]}' for h in headers]) if headers else ''

    return {
        "headers": header_str,
        "char_count": len(chunk),
        "word_count": len(chunk.split())
    }
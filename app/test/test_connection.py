# import httpx
# import asyncio
# async def test_connection():
#     async with httpx.AsyncClient() as client:
#         try:
#             response = await client.get("http://localhost:8051/sse")
#             print(f"状态码: {response.status_code}")
#             print(f"响应内容: {response.text}")
#         except Exception as e:
#             print(f"连接错误: {e}")

# asyncio.run(test_connection())


import requests

response = requests.get(
    "http://127.0.0.1:8051/sse", 
    headers={"Accept": "text/event-stream"},
    stream=True
)
print(f"状态码: {response.status_code}")
print(f"头信息: {response.headers}")
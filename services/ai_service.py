# import os
# import json
# import logging
# import asyncio
# from typing import List
# from dotenv import load_dotenv
# from groq import Groq

# load_dotenv()
# GROQ_API_KEY = os.environ.get('GROQ_API_KEY')
# client = Groq(api_key=GROQ_API_KEY)

# def get_ai_recommend_words(full_text: list[str], age: int, word_count: int) -> List[str]:
#     plain_text = "".join(full_text)
#     chat_completion = client.chat.completions.create(
#         model="llama-3.1-8b-instant",
#         messages=[
#             {
#                 "role": "system",
#                 "content": (
#                             f"你是一位專業的國文老師。任務是從文章中挑選出 {word_count} 個最重要的詞彙。\n"
#                             "請嚴格遵守以下規則：\n"
#                             "1. 數量精確：只能挑選 {word_count} 個詞\n"
#                             "2. 推薦的詞彙須符合 {age} 歲可讀懂的範圍\n"
#                             "3. 格式固定：必須回傳 JSON 物件，格式為 {\"words\": [\"詞1\", \"詞2\", \"詞3\", \"詞4\", \"詞5\"]}\n"
#                             "4. 排除廢話：不要回傳任何 JSON 以外的文字。"
#                         )
#             },
#             {
#                 "role": "user",
#                 "content": f"請從以下文章中挑選{word_count}個適合{age}歲的詞彙：\n\n{plain_text}"
#             }
#         ],
#         response_format={"type": "json_object"}
#     )
#     response = chat_completion.choices[0].message.content
#     try:
#         data = json.loads(response)
#         word_list = data["words"][:word_count]
#     except json.JSONDecodeError:
#         logging.warning('The response is not in valid JSON format')
#         word_list = []
#     return word_list


# async def get_ai_recommend_words_async(
#     full_text,
#     age,
#     word_count
# ):
#     return await asyncio.to_thread(
#         get_ai_recommend_words,
#         full_text,
#         age,
#         word_count
#     )
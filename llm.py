#!llm.py
import os
from dotenv import load_dotenv # type: ignore
from openai import AsyncOpenAI # type: ignore
from typing import Optional

load_dotenv()

API_KEY = os.getenv('XAI_API_KEY')
BASE_URL = 'https://api.x.ai/v1'
MODEL = 'grok-4'

with open('clean.prompt', 'r', encoding='utf-8') as f:
    SYSTEM_PROMPT = f.read()

async def call_llm(default: str) -> Optional[str]:
    """Calls the xAI Grok API asynchronously to clean the string.

    Args:
        default: The default string as user input.

    Returns:
        The cleaned string or None on error.
    """
    if not API_KEY:
        # Fallback: return the input as-is to avoid None during local tests
        return default
    client = AsyncOpenAI(api_key=API_KEY, base_url=BASE_URL)
    try:
        response = await client.chat.completions.create(
            model=MODEL,
            messages=[
                {'role': 'system', 'content': SYSTEM_PROMPT},
                {
                    'role': 'user',
                    'content': f"One line in → one line out. Clean this exactly as instructed. Return only the cleaned line, no quotes, no punctuation changes beyond rules, no extra whitespace. Input: {default}"
                }
            ],
            temperature=0.0,
            max_tokens=1000
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f"Error calling LLM: {e}")
        return None
#!llm.py
import os
from dotenv import load_dotenv # type: ignore
from openai import AsyncOpenAI # type: ignore
from typing import Optional
from config import getXaiApiKey

load_dotenv()

API_KEY = getXaiApiKey()
BASE_URL = 'https://api.x.ai/v1'
MODEL = 'grok-3-mini'

with open('clean.prompt', 'r', encoding='utf-8') as f:
    SYSTEM_PROMPT = f.read()

async def call_llm(default: str, max_retries: int = 3) -> str:
    """Calls the xAI Grok API asynchronously to clean the string.

    This function will ALWAYS call the LLM and wait for a response.
    There is NO fallback to original text - the workflow must go through LLM processing.

    Args:
        default: The default string as user input.
        max_retries: Maximum number of retries on failure (default: 3)

    Returns:
        The cleaned string from LLM.

    Raises:
        RuntimeError: If API key is not available
        Exception: If LLM call fails after all retries
    """
    if not API_KEY:
        raise RuntimeError("LLM API key is not available. Cannot proceed without LLM processing.")

    client = AsyncOpenAI(api_key=API_KEY, base_url=BASE_URL)
    last_exception = None

    for attempt in range(max_retries + 1):
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
            last_exception = e
            error_msg = str(e).lower()

            # Check if it's a rate limit error (429)
            if "429" in error_msg or "rate limit" in error_msg:
                if attempt < max_retries:
                    wait_time = (2 ** attempt) * 60  # Exponential backoff in seconds
                    print(f"LLM rate limited. Waiting {wait_time} seconds before retry {attempt + 1}/{max_retries}")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    raise RuntimeError(f"LLM rate limit exceeded after {max_retries} retries. Cannot proceed without LLM processing.")
            else:
                # For other errors, retry with shorter backoff
                if attempt < max_retries:
                    wait_time = (2 ** attempt) * 10  # Shorter backoff for other errors
                    print(f"LLM error: {e}. Waiting {wait_time} seconds before retry {attempt + 1}/{max_retries}")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    raise RuntimeError(f"LLM call failed after {max_retries} retries: {e}")

    # This should never be reached, but just in case
    raise last_exception or RuntimeError("LLM call failed for unknown reason")
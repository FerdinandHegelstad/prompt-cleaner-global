"""LLM client for cleaning raw prompt text via xAI Grok."""

import asyncio
from typing import Optional
from openai import AsyncOpenAI
from config import getXaiApiKey, getXaiBaseUrl, getXaiModel

with open('prompts/clean.prompt', 'r', encoding='utf-8') as f:
    SYSTEM_PROMPT = f.read()

def _make_client(api_key: Optional[str] = None, base_url: Optional[str] = None) -> AsyncOpenAI:
    """Create an AsyncOpenAI client with proper configuration."""
    key = api_key or getXaiApiKey()
    if not key:
        raise RuntimeError("Missing XAI_API_KEY (set in Streamlit secrets or env).")
    return AsyncOpenAI(api_key=key, base_url=base_url or getXaiBaseUrl())

async def call_llm(
    raw_text: str,
    *,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    model: Optional[str] = None,
    max_retries: int = 3,
) -> str:
    """Call the xAI Grok API to clean a raw prompt string.

    Always calls the LLM -- there is no fallback to the original text.

    Args:
        raw_text: The raw, unprocessed string to clean.
        api_key: Optional API key override.
        base_url: Optional base URL override.
        model: Optional model name override.
        max_retries: Maximum retry attempts on failure.

    Returns:
        The cleaned string from the LLM.

    Raises:
        RuntimeError: If API key is missing or all retries exhausted.
    """
    client = _make_client(api_key, base_url)
    mdl = model or getXaiModel()
    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            response = await client.chat.completions.create(
                model=mdl,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",
                     "content": ("One line in → one line out. Clean this exactly as instructed. "
                                 "Return only the cleaned line, no quotes, no punctuation changes beyond rules, "
                                 f"no extra whitespace. Input: {raw_text}")},
                ],
                temperature=0.0,
                max_tokens=1000,
            )
            return response.choices[0].message.content

        except Exception as e:
            last_exception = e
            error_msg = str(e).lower()

            if "429" in error_msg or "rate limit" in error_msg:
                if attempt < max_retries:
                    wait_time = (2 ** attempt) * 60
                    print(f"LLM rate limited. Waiting {wait_time}s before retry {attempt + 1}/{max_retries}")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    raise RuntimeError(f"LLM rate limit exceeded after {max_retries} retries.")
            else:
                if attempt < max_retries:
                    wait_time = (2 ** attempt) * 10
                    print(f"LLM error: {e}. Waiting {wait_time}s before retry {attempt + 1}/{max_retries}")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    raise RuntimeError(f"LLM call failed after {max_retries} retries: {e}")

    raise last_exception or RuntimeError("LLM call failed for unknown reason")

def call_llm_sync(*args, **kwargs) -> str:
    """Synchronous wrapper for call_llm that handles event loop issues."""
    try:
        return asyncio.run(call_llm(*args, **kwargs))
    except RuntimeError as e:
        if "asyncio.run() cannot be called" in str(e):
            loop = asyncio.new_event_loop()
            import asyncio as _a
            try:
                _a.set_event_loop(loop)
                return loop.run_until_complete(call_llm(*args, **kwargs))
            finally:
                loop.close()
        raise

#!llm.py
import asyncio
from typing import Optional
from openai import AsyncOpenAI
from config import getXaiApiKey, getXaiBaseUrl, getXaiModel

with open('clean.prompt', 'r', encoding='utf-8') as f:
    SYSTEM_PROMPT = f.read()

def _make_client(api_key: Optional[str] = None, base_url: Optional[str] = None) -> AsyncOpenAI:
    """Create an AsyncOpenAI client with proper configuration."""
    key = api_key or getXaiApiKey()
    if not key:
        raise RuntimeError("Missing XAI_API_KEY (set in Streamlit secrets or env).")
    return AsyncOpenAI(api_key=key, base_url=base_url or getXaiBaseUrl())

async def call_llm(
    default: str,
    *,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    model: Optional[str] = None,
    max_retries: int = 3,
) -> str:
    """Calls the xAI Grok API asynchronously to clean the string.

    This function will ALWAYS call the LLM and wait for a response.
    There is NO fallback to original text - the workflow must go through LLM processing.

    Args:
        default: The default string as user input.
        api_key: Optional API key (will use config if not provided)
        base_url: Optional base URL (will use config if not provided)
        model: Optional model name (will use config if not provided)
        max_retries: Maximum number of retries on failure (default: 3)

    Returns:
        The cleaned string from LLM.

    Raises:
        RuntimeError: If API key is not available
        Exception: If LLM call fails after all retries
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
                                 f"no extra whitespace. Input: {default}")},
                ],
                temperature=0.0,
                max_tokens=1000,
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
"""
LLM query module for Memgraph.

Provides llm.complete(text) to send text to an LLM and get a completion.
Uses LiteLLM for model-agnostic completion (OpenAI, Anthropic, Ollama, etc.).

Requires: pip install litellm
Configure: set OPENAI_API_KEY (or provider-specific env), optionally LITELLM_MODEL,
and for local providers like Ollama use config api_base (e.g. http://localhost:11434).
"""

import os

import mgp

try:
    from litellm import completion as litellm_completion

    HAS_LITELLM = True
except ImportError:
    HAS_LITELLM = False

try:
    from litellm import exceptions as litellm_exceptions
except ImportError:
    litellm_exceptions = None

_DEFAULT_SYSTEM_PROMPT = "Complete the following text."


@mgp.function
def complete(
    text: str,
    config: mgp.Map = {},
) -> str:
    """
    Send text to an LLM and return the completion (e.g. a summary).

    Args:
        text: Input text to summarize or complete (e.g. concatenated node texts).
        config: Optional configuration dictionary. Keys:
        model: Model name (e.g. ollama/llama2, openai/gpt-4o-mini).
        api_base: Base URL for the API (e.g. http://localhost:11434 for Ollama).
        system_prompt: System prompt for the completion.

    Returns:
        The model's completion as a string.

    Environment:
        Set API key for your provider, e.g. OPENAI_API_KEY, ANTHROPIC_API_KEY.
        Optional: LITELLM_MODEL to set default model.

    Example Cypher (Ollama):
        RETURN llm.complete("Hello", {model: "ollama/llama2", api_base: "http://localhost:11434"});
    """
    if not HAS_LITELLM:
        raise Exception("llm.complete requires litellm.")

    text = (text or "").strip()
    if not text:
        return ""

    effective_model = config.get("model", os.environ.get("LITELLM_MODEL", "gpt-5-mini"))
    effective_system = config.get("system_prompt", _DEFAULT_SYSTEM_PROMPT)
    api_base = config.get("api_base") or os.environ.get("LITELLM_API_BASE")

    messages = [
        {"role": "system", "content": effective_system},
        {"role": "user", "content": text},
    ]

    completion_kwargs = {
        "model": effective_model,
        "messages": messages,
    }
    if api_base is not None and str(api_base).strip():
        completion_kwargs["api_base"] = str(api_base).strip()

    try:
        response = litellm_completion(**completion_kwargs)
    except Exception as e:
        if litellm_exceptions is not None:
            if isinstance(e, litellm_exceptions.BadRequestError):
                raise Exception(
                    f"llm.complete: invalid model or request (model={effective_model}). "
                    "Use a valid model name and provider, e.g. openai/gpt-4o-mini. "
                    f"Details: {e}"
                ) from None
            if isinstance(e, litellm_exceptions.AuthenticationError):
                raise Exception(
                    f"llm.complete: authentication failed for model {effective_model}. "
                    "Check API key and provider env vars (e.g. OPENAI_API_KEY). "
                    f"Details: {e}"
                ) from None
            if isinstance(e, litellm_exceptions.RateLimitError):
                raise Exception(
                    f"llm.complete: rate limit exceeded for model {effective_model}. " f"Details: {e}"
                ) from None
            if isinstance(e, litellm_exceptions.APIConnectionError):
                raise Exception(
                    f"llm.complete: cannot reach API for model {effective_model}. "
                    "Check network, API base URL, and that the service is running. "
                    f"Details: {e}"
                ) from None
        raise Exception(f"llm.complete failed (model={effective_model}): {e}") from None

    choice = response.choices[0] if response.choices else None
    if not choice or not getattr(choice.message, "content", None):
        return ""

    return (choice.message.content or "").strip()

"""
LLM query module for Memgraph.

Provides llm.complete(text) to send text to an LLM and get a completion.
Uses LiteLLM for model-agnostic completion (OpenAI, Anthropic, Ollama, etc.).

Requires: pip install litellm
Configure: set OPENAI_API_KEY (or provider-specific env) and optionally LITELLM_MODEL.
"""

import json
import os

import mgp

try:
    from litellm import completion as litellm_completion

    HAS_LITELLM = True
except ImportError:
    HAS_LITELLM = False

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
        config: Optional configuration dictionary.

    Returns:
        The model's completion as a string.

    Environment:
        Set API key for your provider, e.g. OPENAI_API_KEY, ANTHROPIC_API_KEY.
        Optional: LITELLM_MODEL to set default model.

    Example Cypher:
        RETURN llm.complete("{{dummy_example}}", config);
    """
    if not HAS_LITELLM:
        raise Exception("llm.complete requires litellm.")

    text = (text or "").strip()
    if not text:
        return ""

    effective_model = config.get("model", os.environ.get("LITELLM_MODEL", "gpt-5-mini"))
    effective_system = config.get("system_prompt", _DEFAULT_SYSTEM_PROMPT)

    messages = [
        {"role": "system", "content": effective_system},
        {"role": "user", "content": text},
    ]

    response = litellm_completion(
        model=effective_model,
        messages=messages,
    )

    choice = response.choices[0] if response.choices else None
    if not choice or not getattr(choice.message, "content", None):
        return ""

    return (choice.message.content or "").strip()

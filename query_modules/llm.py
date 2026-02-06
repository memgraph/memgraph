"""
LLM query module for Memgraph.

Provides llm.complete(text) to send text to an LLM and get a completion (e.g. summary).
Uses LiteLLM for model-agnostic completion (OpenAI, Anthropic, Ollama, etc.).

Requires: pip install litellm
Configure: set OPENAI_API_KEY (or provider-specific env) and optionally LITELLM_MODEL.
"""

import os

import mgp

try:
    from litellm import completion as litellm_completion

    HAS_LITELLM = True
except ImportError:
    HAS_LITELLM = False

_DEFAULT_SYSTEM_PROMPT = "Summarize the following text concisely. " "Return only the summary, no preamble."


@mgp.function
def complete(
    text: str,
    model: mgp.Nullable[str] = None,
    system_prompt: mgp.Nullable[str] = None,
) -> str:
    """
    Send text to an LLM and return the completion (e.g. a summary).

    Args:
        text: Input text to summarize or complete (e.g. concatenated node texts).
        model: Optional model name (e.g. "gpt-4o-mini", "claude-3-haiku", "ollama/llama2").
               If not set, uses env LITELLM_MODEL or litellm default.
        system_prompt: Optional system instruction. If not set, asks for a concise summary.

    Returns:
        The model's completion as a string.

    Environment:
        Set API key for your provider, e.g. OPENAI_API_KEY, ANTHROPIC_API_KEY.
        Optional: LITELLM_MODEL to set default model.

    Example Cypher:
        MATCH (n) WHERE n.community_id IS NOT NULL
        WITH n.community_id AS c_id, count(n) AS c_count, collect(n) AS c_members
        WITH c_id, c_count, c_members,
             llm.complete(reduce(s = "", m IN c_members | s + m.text + "; ")) AS c_summary
        MERGE (community:Community {id: c_id, nodes_count: c_count, summary: c_summary})
        WITH community, c_members
        UNWIND c_members AS c_member
        MERGE (c_member)-[:BELONGS_TO]->(community)
        RETURN community.id AS community_id, community.summary AS community_summary;
    """
    if not HAS_LITELLM:
        raise Exception("llm.complete requires litellm.")

    text = (text or "").strip()
    if not text:
        return ""

    effective_model = model or os.environ.get("LITELLM_MODEL", "gpt-4o-mini")
    effective_system = system_prompt or _DEFAULT_SYSTEM_PROMPT

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

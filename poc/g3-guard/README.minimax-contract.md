# G3 · MiniMax M3 offline contract

Versioned experimental extension of the original synthetic-credit budget gate. Run: `node --test poc/g3-guard/*.test.mjs`. No SDK, provider key, true network endpoint or Lunar access.

Grounded protocol (official): https://platform.minimax.io/docs/api-reference/text-chat-openai
- Model: MiniMax-M3; OpenAI-compatible POST /v1/chat/completions.
- Explicit max_completion_tokens cap; no streaming/tools/priority/unsupported fields in this lab.
- Success: usage.prompt_tokens, usage.completion_tokens, usage.prompt_tokens_details.cached_tokens, usage.total_tokens, base_resp.status_code=0.
- Cache is charged at full (conservative synthetic) input rate; no assumption about subscribed quota conversion.
- Fail closed on absent or implausible usage; reserve worst case before forwarding; hold worst case after network ambiguity.

**Crucial limitation**: `estimateInputUpperBound` is an injected **mock-only** server-side callback. It is NOT a real MiniMax tokenizer and does not prove hidden/tool context bounds. The official /v1/responses/input_tokens estimates a Responses API request, not necessarily this Chat Completions payload. Until the actual runtime/provider request format, upper bound, streaming behavior, tools, plan-credit conversion, authenticated run isolation, and durable multi-worker ledger are verified, G3 remains BLOCKED for Lunar.

Provider is always explicitly loopback (127.0.0.1). Never use provider credentials or production hosts. No new costs beyond standard public GitHub Actions runner, no artifacts. Branch is evidence only. Do not merge or deploy.

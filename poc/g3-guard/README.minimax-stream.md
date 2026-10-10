# G3 · MiniMax SSE lab · 2026-10-10

A single incremental suite exercises mock Server-Sent Events (SSE) while keeping the original 23 passing tests untouched.

Run: node --test poc/g3-guard/minimax-stream.test.mjs

OFFICIAL SOURCES
- https://platform.minimax.io/docs/api-reference/text-chat-openai (stream, stream_options, max_completion_tokens, usage).
- https://platform.minimax.io/docs/api-reference/responses-input-tokens (the only explicitly documented token-count endpoint; it is Responses-specific and does NOT certify the Chat Completions input).
- https://solutions.minimax.cn/debug/text (provider example: stream_options.include_usage).

LAB CONTRACT
- Only MiniMax-M3, text messages and loopback fake server. Gate injects stream_options.include_usage.
- Before forwarding, a trusted SERVER-SIDE callback must provide a positive upper bound. In the tests it is an injected mock; it is NOT a real tokenizer or MiniMax attestation. No bound => deny 503; cannot claim full G3 hard cap.
- Reserve synthetic credits for input bound plus output cap; in-flight reservations are included and concurrent requests fail preflight.
- Forward SSE delta fragments and delay terminal [DONE] until final usage is validated; missing usage, interruptions or overruns retain full reservation, and send an explicit g3_error event when the stream already started.
- No models, credentials, authentication, tool calls, durable ledger, real provider pricing or Lunar changes.

REMAINING GATE
Reliable bounds for the actual OpenCode wire request including hidden prompts/tool payloads are UNVERIFIED; stream compatibility with real OpenCode is UNVERIFIED. Do not substitute character-count or a Responses token estimate silently. Reserve a verified model-wide context maximum only when actual provider constraints and billing semantics are proven; it may be too conservative. Before production: authenticate, persist atomic ledger, restrict egress, verify in-flight billing and obtain explicit approval for any host changes or credits.

Earlier 23 tests remain documented at https://github.com/theatomika/multica/actions/runs/38047664476 and are intentionally not rerun in this incremental suite.

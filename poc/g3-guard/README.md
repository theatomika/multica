# G3 · Budget gate laboratory (mock only)

Standalone Node.js 22 prototype. **Not a production gateway.** No provider credentials, Lunar integrations, real LLM calls, persistent infrastructure or chargeable APIs.

## What it enforces in the laboratory

- Each run has a fixed **integer synthetic credit** budget set server-side, not by the requester.
- Before forwarding, the gate reserves the **worst-case** credit amount for prompt bytes (simulated input-token upper bound) plus the simulated provider's enforced maximum output tokens.
- Reservations are synchronous; concurrent in-flight calls cannot collectively exceed `limitCredits`.
- Verified mock usage settles the actual amount and releases unused reservation.
- A failed, interrupted, unverified or over-budget mock response consumes the **entire reserved amount** (fail closed). No automatic retries.
- All HTTP traffic is loopback-only. No caller-selected upstream URL and no real provider endpoint.

## Important limitations

**Synthetic credits are not USD and do not represent Xiaomi or MiniMax billing.** Prompt byte count is *only* a conservative input-token bound in this controlled mock setup. A real provider may add hidden/context tokens, charge cache differently, vary model price, omit usage, process retries, bill in-flight requests after disconnect, or ignore output caps. A real deployment would need provider-verified request bounds, charging semantics, a durable atomic ledger across worker processes, authentication and run isolation, and a controlled egress boundary. This prototype does **not** prove a monetary hard cap or eligibility to connect OpenCode/Lunar.

No changes to B4 PR #1 or to the runtime. G3 stays BLOCKED for real Lunar until the real provider route and hard enforcement are verified and separately approved.

## Reproduce

`node --test poc/g3-guard/budget-guard.test.mjs` — 10 tests using a local HTTP mock provider. CI runs this once on a standard public GitHub-hosted Ubuntu runner with an 8-minute job cap; no artifact upload or caches.

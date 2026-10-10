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

## G3 · Native OpenCode preflight (offline, provider.id scope)

`poc/g3-guard/opencode-preflight.test.mjs` ports the synchronous provider-policy decision in `anomalyco/opencode@9b4ec5714d481559990db0a816d5dec19541a814` (`packages/core/src/managed-policy.ts`, `packages/core/src/util/wildcard.ts`, and the `opencode.config.policy` plugin in `packages/core/src/config/plugin/policy.ts`) and proves the rejection path. The fixture uses the canonical provider id `minimax` (`packages/ai/src/providers/minimax.ts`: `export const id = ProviderID.make("minimax")`); the model id `MiniMax-M3` is exercised only as a negative case.

- Negative: `provider.use deny MiniMax-M3` does NOT block the provider whose id is `minimax`.
- `provider.use deny minimax` blocks both HTTP and WebSocket upstream paths.
- 25 concurrent in-flight attempts all blocked; `BudgetLedger` snapshot unchanged.
- 50-call retry storm never reaches the handler; reservations stay at zero.
- Organization-managed deny overrides user-authored allow (last-match-wins, per the v2 policy spec).
- Negative control: no statement → exactly one send recorded; allow-override after wildcard deny still routes correctly.

Eight focused tests, no network egress, no real provider. Run with:

```
node --test poc/g3-guard/opencode-preflight.test.mjs
```

This file does **not** declare G3 real PASS and is **not** a credit budget. It only verifies the catalog-time preflight for provider ids. The per-run accumulated budget control, provider route, hard caps, durable ledger, authentication and run isolation remain unverified and require a separate approval.

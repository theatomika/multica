// G3 laboratory: per-run accumulated budget guard for the MiniMax Messages
// (Anthropic-compatible) HTTP route. Lab only: no real provider, no creds.
//
// This module is INDEPENDENT of the global catalog-level provider.use deny
// in opencode-preflight.mjs. That hook remains the global switch; this guard
// enforces a per-run (per-execution) accumulated credit budget without
// touching provider.use. BudgetLedger from budget-guard.mjs is reused as the
// atomic, fail-closed reservation engine.
//
// What this guard enforces (pre-send):
//   1. The caller MUST supply an explicit run identity. The guard never assumes
//      that a session id equals a run id; the run identity is the operator's
//      per-execution label. The lab helper RunIdentity.resolve() accepts a
//      primary candidate and a fallback session id only if the operator
//      passes `allowSessionFallback: true`. Without an acceptable identity the
//      request is denied with `unknown_run` and ZERO upstream calls happen.
//   2. The run MUST be opened with an explicit positive integer credit limit.
//      Unknown runs are denied. Limit < 1 is denied. Without a limit the guard
//      is fail-closed (`missing_limit`).
//   3. The estimator MUST return a positive safe integer upper bound on input
//      tokens before the upstream is contacted. A non-positive or non-integer
//      bound → `unverified_input_bound`, ZERO upstream calls.
//   4. The reservation MUST succeed (>= 2 tokens credit). The guard reserves
//      atomically and synchronously; concurrent reservations across retries
//      cannot oversubscribe the run.
//   5. Only THEN the upstream is contacted. The request body and headers
//      (including x-g3-run-id) are sent on a single counted HTTP POST to the
//      configured loopback simulator.
//   6. Output cap: max_tokens from the caller is capped server-side; anything
//      above the cap is denied at handler time, never silently truncated.
//   7. Settlement is terminal: only a complete SSE stream ending in
//      message_stop is settled by usage; partial / disconnect / no-usage → the
//      full reserved amount is charged (fail closed). Retries OPEN a fresh
//      reservation; the prior one was already charged or settled.
//
// What this guard is NOT:
//   - A monetary hard cap. It charges SYNTHETIC credits against the run
//     ledger. The pricing formula is a lab constant; it does NOT model
//     provider billing, cache policy, plan credits, retries, in-flight
//     disconnect accounting, or USD/EUR rates. There is no calibrated
//     bytes-to-token factor; the estimator is operator-supplied.
//   - A replacement for the global provider.use deny. The catalog preflight
//     in opencode-preflight.mjs continues to gate which provider IDs are
//     reachable at all.
//   - A way to skip the upstream. Reaches it exactly once per reservation
//     unless the resolver rejects, the estimator is unavailable, the
//     reservation is denied, or the run is missing.

import http from 'node:http'
import { BudgetLedger } from './budget-guard.mjs'

const isPosInt = (n) => Number.isSafeInteger(n) && n > 0
const isNonNegInt = (n) => Number.isSafeInteger(n) && n >= 0

function sendJson(res, status, payload) {
  res.writeHead(status, { 'content-type': 'application/json', 'cache-control': 'no-store' })
  res.end(JSON.stringify(payload))
}

// RunIdentity: never confuses sessionID with runID. The lab accepts a
// caller-supplied run id AND/OR a session id. The fallback to session is
// opt-in; absent that, a missing run id is fatal.
export function resolveRunIdentity({ runId, sessionId }, { allowSessionFallback = false } = {}) {
  if (typeof runId === 'string' && /^[a-zA-Z0-9_-]{1,64}$/.test(runId)) {
    return { ok: true, runId, source: 'runId' }
  }
  if (allowSessionFallback && typeof sessionId === 'string' && /^[a-zA-Z0-9_-]{1,64}$/.test(sessionId)) {
    return { ok: true, runId: sessionId, source: 'sessionId_fallback' }
  }
  return { ok: false, reason: 'unknown_run' }
}

async function readSmallJson(req, maxBytes) {
  let bytes = 0
  const chunks = []
  for await (const chunk of req) {
    bytes += chunk.length
    if (bytes > maxBytes) throw new Error('request_too_large')
    chunks.push(chunk)
  }
  if (bytes === 0) throw new Error('empty_body')
  return JSON.parse(Buffer.concat(chunks).toString('utf8'))
}

// Parse a minimal Anthropic-style SSE stream and accumulate terminal usage.
// Counts only events with the canonical terminal marker message_stop.
async function consumeAnthropicSse(stream, bound, maxOutput) {
  let buffer = ''
  let size = 0
  const decoder = new TextDecoder()
  let start = null
  let finish = null
  let stopped = false
  for await (const chunk of stream) {
    size += chunk.byteLength
    if (size > 65536) throw new Error('oversize_stream')
    buffer = (buffer + decoder.decode(chunk, { stream: true })).replace(/\r\n/g, '\n')
    if (buffer.length > 65536) throw new Error('oversize_buffer')
    while (buffer.includes('\n\n')) {
      const i = buffer.indexOf('\n\n')
      const lines = buffer.slice(0, i).split('\n')
      buffer = buffer.slice(i + 2)
      let type = ''
      let data = ''
      for (const l of lines) {
        if (l.startsWith(':') || l === '') continue
        if (l.startsWith('event:')) type = l.slice(6).trim()
        else if (l.startsWith('data:')) data += l.slice(5).trim()
        else throw new Error('unsupported_sse')
      }
      if (type && data) {
        const body = JSON.parse(data)
        if (type === 'message_start' && body?.type === 'message_start' && !start) start = body
        if (type === 'message_delta' && body?.type === 'message_delta' && start && !stopped) finish = body
        if (type === 'message_stop' && body?.type === 'message_stop' && start && finish) stopped = true
      }
    }
  }
  if (decoder.decode() || buffer.trim()) throw new Error('incomplete_sse')
  if (!stopped) throw new Error('no_terminal_message_stop')
  const s = start?.message?.usage
  const f = finish?.usage
  if (!s || !f) throw new Error('missing_usage')
  if (!isNonNegInt(s.input_tokens) || !isNonNegInt(s.cache_read_input_tokens ?? 0)
      || !isNonNegInt(s.cache_creation_input_tokens ?? 0) || !isNonNegInt(f.output_tokens)) {
    throw new Error('non_integer_usage')
  }
  const totalInput = s.input_tokens + (s.cache_read_input_tokens ?? 0) + (s.cache_creation_input_tokens ?? 0)
  if (!isPosInt(totalInput) || totalInput > bound || f.output_tokens > maxOutput) {
    throw new Error('out_of_bound_usage')
  }
  return { input_tokens: totalInput, output_tokens: f.output_tokens }
}

// createRunBudgetGuardServer: HTTP intercept for POST /v1/run on the
// MiniMax Messages route. The guard is the boundary that enforces identity,
// limit, estimator and reservation BEFORE the configured loopback simulator
// is contacted. Anything missing → ZERO upstream sends.
export function createRunBudgetGuardServer({
  ledger,
  simulatorURL,
  trustedInputUpperBound, // async (body) => positive integer
  maxOutputTokens = 16,
  timeoutMs = 3000,
  allowSessionFallback = false,
}) {
  if (!(ledger instanceof BudgetLedger)) throw new TypeError('ledger must be a BudgetLedger instance')
  const upstream = new URL(simulatorURL)
  if (upstream.protocol !== 'http:' || !['127.0.0.1', '[::1]'].includes(upstream.hostname)
      || upstream.username || upstream.password || upstream.search || upstream.hash) {
    throw new Error('Run budget guard only accepts an explicit HTTP loopback simulator')
  }
  if (typeof trustedInputUpperBound !== 'function') throw new TypeError('trustedInputUpperBound must be a function')
  if (!isPosInt(maxOutputTokens)) throw new TypeError('maxOutputTokens must be a positive safe integer')
  if (!isPosInt(timeoutMs)) throw new TypeError('timeoutMs must be a positive safe integer')

  return http.createServer(async (req, res) => {
    if (req.method !== 'POST' || req.url !== '/v1/run') return sendJson(res, 404, { error: 'not_found' })

    let payload
    try { payload = await readSmallJson(req, 16384) }
    catch { return sendJson(res, 400, { error: 'invalid_request' }) }
    if (!payload || typeof payload !== 'object') return sendJson(res, 400, { error: 'invalid_request' })

    // 1. Run identity: NEVER assume sessionId == runId.
    const ident = resolveRunIdentity(
      { runId: payload.runId, sessionId: payload.sessionId },
      { allowSessionFallback },
    )
    if (!ident.ok) return sendJson(res, 403, { error: ident.reason })

    // 2. Run MUST exist with explicit limit (fail-closed when missing).
    if (!ledger.hasRun(ident.runId)) return sendJson(res, 403, { error: 'unknown_run' })

    // 3. Caller shape (model + stream + max_tokens) before we trust the body.
    if (typeof payload.prompt !== 'string' || payload.prompt.length === 0) {
      return sendJson(res, 400, { error: 'invalid_request' })
    }
    if (!isPosInt(payload.maxOutputTokens) || payload.maxOutputTokens > maxOutputTokens) {
      return sendJson(res, 400, { error: 'output_cap_exceeded' })
    }

    // 4. Estimator: positive integer upper bound BEFORE upstream.
    let bound
    try { bound = await trustedInputUpperBound({ prompt: payload.prompt, model: payload.model }) }
    catch { return sendJson(res, 503, { error: 'unverified_input_bound' }) }
    if (!isPosInt(bound)) return sendJson(res, 503, { error: 'unverified_input_bound' })

    // 5. Atomic reservation. Concurrent retries in the same run cannot
    // oversubscribe: the ledger rejects the second ticket synchronously.
    let ticket
    try { ticket = ledger.reserve(ident.runId, bound, payload.maxOutputTokens) }
    catch { return sendJson(res, 400, { error: 'invalid_budget_bounds' }) }
    if (!ticket) return sendJson(res, 429, { error: 'budget_exhausted' })

    // 6. NOW and only now we contact the loopback simulator.
    try {
      const upstreamRes = await fetch(upstream, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'anthropic-version': '2023-06-01',
          'x-g3-run-id': ident.runId,
          'x-g3-allow-session-fallback': allowSessionFallback ? '1' : '0',
        },
        body: JSON.stringify({
          model: payload.model ?? 'MiniMax-M3',
          max_tokens: payload.maxOutputTokens,
          stream: true,
          system: payload.system ?? '',
          messages: [{ role: 'user', content: payload.prompt }],
        }),
        signal: AbortSignal.timeout(timeoutMs),
      })
      if (upstreamRes.status !== 200
          || !upstreamRes.headers.get('content-type')?.includes('text/event-stream')) {
        throw new Error('upstream_protocol_error')
      }
      const usage = await consumeAnthropicSse(upstreamRes.body, bound, payload.maxOutputTokens)
      const charged = ledger.settle(ticket, usage)
      return sendJson(res, 200, {
        verified: true,
        syntheticCredits: charged,
        runIdSource: ident.source,
      })
    } catch {
      // Fail closed: any partial/disconnect/no-usage path charges the FULL
      // reservation. Retries MUST open a new reservation.
      ledger.chargeFull(ticket)
      return sendJson(res, 502, { error: 'upstream_or_usage_unverified' })
    }
  })
}

// RunBudgetGuard: the same enforcement boundary expressed as a plain object
// the rest of the lab can unit-test (no HTTP needed). This is what the test
// suite calls directly to prove pre-send interception in isolation.
export class RunBudgetGuard {
  constructor({ ledger, trustedInputUpperBound, maxOutputTokens = 16,
                allowSessionFallback = false, now = () => Date.now() }) {
    if (!(ledger instanceof BudgetLedger)) throw new TypeError('ledger must be a BudgetLedger instance')
    if (typeof trustedInputUpperBound !== 'function') throw new TypeError('trustedInputUpperBound must be a function')
    if (!isPosInt(maxOutputTokens)) throw new TypeError('maxOutputTokens must be a positive safe integer')
    this.ledger = ledger
    this.trustedInputUpperBound = trustedInputUpperBound
    this.maxOutputTokens = maxOutputTokens
    this.allowSessionFallback = allowSessionFallback
    this.now = now
  }

  // admit performs the full pre-send check and returns a structured result.
  // Upstream MUST only be invoked after admit.ok === true.
  admit({ runId, sessionId, prompt, model, maxOutputTokens }) {
    const ident = resolveRunIdentity(
      { runId, sessionId },
      { allowSessionFallback: this.allowSessionFallback },
    )
    if (!ident.ok) return { ok: false, status: 403, error: ident.reason }
    if (!this.ledger.hasRun(ident.runId)) {
      return { ok: false, status: 403, error: 'unknown_run' }
    }
    if (typeof prompt !== 'string' || prompt.length === 0) {
      return { ok: false, status: 400, error: 'invalid_request' }
    }
    if (!isPosInt(maxOutputTokens) || maxOutputTokens > this.maxOutputTokens) {
      return { ok: false, status: 400, error: 'output_cap_exceeded' }
    }
    return { ok: true, runId: ident.runId, source: ident.source }
  }

  // reserveIfAffordable calls the estimator synchronously and reserves
  // atomically. Returns the ticket or an error result. Caller MUST NOT
  // contact the upstream before this returns ok.
  async reserveIfAffordable({ runId, prompt, model, maxOutputTokens }) {
    let bound
    try { bound = await this.trustedInputUpperBound({ prompt, model }) }
    catch { return { ok: false, status: 503, error: 'unverified_input_bound' } }
    if (!isPosInt(bound)) return { ok: false, status: 503, error: 'unverified_input_bound' }
    let ticket
    try { ticket = this.ledger.reserve(runId, bound, maxOutputTokens) }
    catch { return { ok: false, status: 400, error: 'invalid_budget_bounds' } }
    if (!ticket) return { ok: false, status: 429, error: 'budget_exhausted' }
    return { ok: true, ticket, bound }
  }
}
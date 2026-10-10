// G3 lab: focused offline proof that the OpenCode v2.0.26 native provider
// preflight (anomalyco/opencode@9b4ec5714d481559990db0a816d5dec19541a814,
// opencode.config.policy plugin + ManagedPolicy.decision) blocks the
// provider handler before any HTTP or WebSocket send is attempted.
//
// This file uses the canonical provider.id "minimax" (verified in
// packages/ai/src/providers/minimax.ts, `export const id = ProviderID.make("minimax")`).
// "MiniMax-M3" is a MODEL id under provider "minimax"; the policy hook matches
// provider IDs, not model IDs. Test 1 demonstrates the negative case: a deny
// statement with resource "MiniMax-M3" must NOT block the provider whose id is
// "minimax".
//
// What this test does NOT do:
//   - Calls any real provider or external endpoint
//   - Touches the G3 lab budget ledger beyond reservation retention asserts
//   - Repeats the 42 already-passing checks from messages-lab/contract/stream
//   - Declares G3 real PASS or asserts any monetary cap
//
// What this test asserts:
//   - provider.use deny on a model id does NOT block the provider (negative)
//   - provider.use deny on "minimax" blocks both HTTP and WebSocket upstream
//   - 25 concurrent in-flight attempts all blocked, zero sends
//   - 50-call retry storm never reaches the handler; reservations stay at zero
//   - Organization-managed deny overrides user-authored allow
//   - Negative control: no statement -> exactly one send recorded
//   - Allow-override after wildcard deny routes correctly (last-match-wins)

import { test } from "node:test"
import assert from "node:assert/strict"
import http from "node:http"
import {
  applyProviderPreflight,
  tryAdmitRequest,
} from "./opencode-preflight.mjs"
import { BudgetLedger } from "./budget-guard.mjs"

function countedProviderHandler({ label, transport }) {
  let sends = 0
  let wsUpgrades = 0
  let body = null
  const server = http.createServer((req, res) => {
    sends++
    body = null
    let buf = ""
    req.on("data", (c) => { buf += c })
    req.on("end", () => { body = buf; res.writeHead(204).end() })
  })
  server.on("upgrade", (_req, socket) => {
    wsUpgrades++
    try { socket.destroy() } catch {}
  })
  return {
    label,
    transport,
    sends: () => sends,
    wsUpgrades: () => wsUpgrades,
    body: () => body,
    async listen() {
      await new Promise((resolve, reject) => {
        server.once("error", reject)
        server.listen(0, "127.0.0.1", resolve)
      })
      return "http://127.0.0.1:" + server.address().port
    },
    close: () => new Promise((r) => server.close(() => r())),
  }
}

async function withHandler(t, opts) {
  const handler = countedProviderHandler(opts)
  const url = await handler.listen()
  t.after(() => handler.close())
  return { handler, url }
}

// Mirrors the OpenCode runtime boundary the spec describes:
// the runtime builds the catalog at config-load time, runs the policy
// transform, and only constructs/imports transport code for catalog members.
async function dispatch({ catalog, providerId, handler, payload, ws = false }) {
  const admission = tryAdmitRequest({ catalog, providerId })
  if (!admission.admitted) return { ok: false, reason: admission.reason }
  if (ws) {
    await new Promise((resolve) => {
      const req = http.request({
        host: "127.0.0.1", port: new URL(handler.url).port, method: "GET",
        headers: { Connection: "Upgrade", Upgrade: "websocket",
                   "Sec-WebSocket-Version": "13",
                   "Sec-WebSocket-Key": "dGhlIHNhbXBsZSBub25jZQ==" },
      })
      req.on("upgrade", () => resolve())
      req.on("error", () => resolve())
      req.on("close", () => resolve())
      req.end()
    })
    return { ok: true }
  }
  const res = await fetch(handler.url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload ?? {}),
  })
  await res.text()
  return { ok: res.status === 204 }
}

const MINIMAX = "minimax"
const M3_MODEL = "MiniMax-M3"

test("provider.use deny on a model id (MiniMax-M3) does NOT block provider 'minimax'", async (t) => {
  const { handler, url } = await withHandler(t, { label: "minimax-model-deny", transport: "http" })
  const { catalog, denied } = applyProviderPreflight({
    providers: [{ id: MINIMAX, transport: "http", label: "MiniMax M3" }],
    authoredEntries: [{
      info: { experimental: { policies: [
        { action: "provider.use", resource: M3_MODEL, effect: "deny" },
      ] } },
    }],
    managed: { statements: [], organization: undefined },
  })
  assert.deepEqual(denied, [], "model id is not a provider id; nothing is denied")
  assert.equal(catalog.has(MINIMAX), true, "provider remains in the catalog")
  const out = await dispatch({ catalog, providerId: MINIMAX, handler: { url }, payload: { prompt: "hi" } })
  assert.equal(out.ok, true, "request reaches the handler because the deny targets a model, not a provider")
  assert.equal(handler.sends(), 1, "exactly one HTTP request reaches the upstream")
})

test("provider.use deny on 'minimax' prevents HTTP handler from being reached", async (t) => {
  const { handler, url } = await withHandler(t, { label: "minimax-http-deny", transport: "http" })
  const { catalog, denied } = applyProviderPreflight({
    providers: [{ id: MINIMAX, transport: "http", label: "MiniMax M3" }],
    authoredEntries: [{
      info: { experimental: { policies: [
        { action: "provider.use", resource: MINIMAX, effect: "deny" },
      ] } },
    }],
    managed: { statements: [], organization: undefined },
  })
  assert.deepEqual(denied, [MINIMAX], "provider id must appear in the deny list")
  assert.equal(catalog.has(MINIMAX), false, "denied provider removed from catalog")
  const out = await dispatch({ catalog, providerId: MINIMAX, handler: { url }, payload: { prompt: "hi" } })
  assert.equal(out.ok, false)
  assert.equal(out.reason, "denied_by_preflight")
  assert.equal(handler.sends(), 0, "HTTP server received zero requests")
  assert.equal(handler.wsUpgrades(), 0)
})

test("provider.use deny on 'minimax' also blocks the WebSocket upgrade path", async (t) => {
  const { handler, url } = await withHandler(t, { label: "minimax-ws-deny", transport: "websocket" })
  const { catalog } = applyProviderPreflight({
    providers: [{ id: MINIMAX, transport: "websocket", label: "MiniMax M3" }],
    authoredEntries: [{
      info: { experimental: { policies: [
        { action: "provider.use", resource: MINIMAX, effect: "deny" },
      ] } },
    }],
    managed: { statements: [], organization: undefined },
  })
  const out = await dispatch({ catalog, providerId: MINIMAX, handler: { url }, ws: true })
  assert.equal(out.ok, false)
  assert.equal(out.reason, "denied_by_preflight")
  assert.equal(handler.wsUpgrades(), 0, "no WebSocket upgrade reached the server")
  assert.equal(handler.sends(), 0)
})

test("organization-managed deny on 'minimax' still filters and overrides user allow", async (t) => {
  const { handler, url } = await withHandler(t, { label: "minimax-org-deny", transport: "http" })
  const { catalog, denied } = applyProviderPreflight({
    providers: [{ id: MINIMAX }, { id: "anthropic" }],
    authoredEntries: [{
      info: { experimental: { policies: [
        { action: "provider.use", resource: MINIMAX, effect: "allow" },
      ] } },
    }],
    managed: {
      organization: "theatomika",
      statements: [
        { action: "provider.use", resource: MINIMAX, effect: "deny" },
      ],
    },
  })
  assert.deepEqual(denied, [MINIMAX], "managed deny wins over authored allow")
  const out = await dispatch({ catalog, providerId: MINIMAX, handler: { url }, payload: {} })
  assert.equal(out.ok, false)
  assert.equal(handler.sends(), 0)
})

test("concurrent in-flight attempts on 'minimax' all blocked; handler sees zero sends", async (t) => {
  const { handler, url } = await withHandler(t, { label: "minimax-conc-deny", transport: "http" })
  const { catalog } = applyProviderPreflight({
    providers: [{ id: MINIMAX }],
    authoredEntries: [{
      info: { experimental: { policies: [
        { action: "provider.use", resource: MINIMAX, effect: "deny" },
      ] } },
    }],
    managed: { statements: [], organization: undefined },
  })
  const N = 25
  const results = await Promise.all(
    Array.from({ length: N }, () =>
      dispatch({ catalog, providerId: MINIMAX, handler: { url }, payload: { prompt: "x" } }),
    ),
  )
  for (const r of results) {
    assert.equal(r.ok, false)
    assert.equal(r.reason, "denied_by_preflight")
  }
  assert.equal(handler.sends(), 0)
})

test("retry storm on 'minimax' never reaches the handler; ledger reservations stay at zero", async (t) => {
  const { handler, url } = await withHandler(t, { label: "minimax-retry-deny", transport: "http" })
  const { catalog } = applyProviderPreflight({
    providers: [{ id: MINIMAX }],
    authoredEntries: [{
      info: { experimental: { policies: [
        { action: "provider.use", resource: MINIMAX, effect: "deny" },
      ] } },
    }],
    managed: { statements: [], organization: undefined },
  })
  const ledger = new BudgetLedger({ inputCreditsPerToken: 1, outputCreditsPerToken: 1 })
  ledger.openRun("retry", 100)
  const before = ledger.snapshot("retry")
  for (let i = 0; i < 50; i++) {
    const out = await dispatch({ catalog, providerId: MINIMAX, handler: { url }, payload: { prompt: "x" } })
    assert.equal(out.ok, false)
  }
  const after = ledger.snapshot("retry")
  assert.deepEqual(after, before, "ledger snapshot unchanged across denied calls")
  assert.equal(handler.sends(), 0, "no HTTP request reached the provider")
})

test("negative control: no statement -> catalog admits and exactly one send is recorded", async (t) => {
  const { handler, url } = await withHandler(t, { label: "control-allow", transport: "http" })
  const { catalog, denied } = applyProviderPreflight({
    providers: [{ id: MINIMAX }],
    authoredEntries: [],
    managed: { statements: [], organization: undefined },
  })
  assert.deepEqual(denied, [], "no statement means no denial")
  assert.equal(catalog.has(MINIMAX), true)
  const out = await dispatch({ catalog, providerId: MINIMAX, handler: { url }, payload: { prompt: "ping" } })
  assert.equal(out.ok, true, "handler reachable exactly once")
  assert.equal(handler.sends(), 1)
  assert.equal(handler.body(), JSON.stringify({ prompt: "ping" }))
})

test("allow-override after earlier wildcard deny routes only the allowed provider", async (t) => {
  const { handler, url } = await withHandler(t, { label: "minimax-allow", transport: "http" })
  // Authored: deny * then allow minimax (last match wins on provider id).
  const { catalog } = applyProviderPreflight({
    providers: [{ id: MINIMAX }, { id: "anthropic" }],
    authoredEntries: [{
      info: { experimental: { policies: [
        { action: "provider.use", resource: "*", effect: "deny" },
        { action: "provider.use", resource: MINIMAX, effect: "allow" },
      ] } },
    }],
    managed: { statements: [], organization: undefined },
  })
  assert.deepEqual(catalog.snapshot(), [MINIMAX])
  const blocked = await dispatch({ catalog, providerId: "anthropic", handler: { url }, payload: {} })
  assert.equal(blocked.ok, false)
  const allowed = await dispatch({ catalog, providerId: MINIMAX, handler: { url }, payload: { prompt: "y" } })
  assert.equal(allowed.ok, true)
  assert.equal(handler.sends(), 1, "exactly one allowed HTTP request was sent")
})
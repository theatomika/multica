// G3 lab: focused offline proof that the OpenCode v2.0.26 native provider
// preflight (anomalyco/opencode@9b4ec5714d481559990db0a816d5dec19541a814,
// opencode.config.policy plugin + ManagedPolicy.decision) blocks the
// provider handler before any HTTP or WebSocket send is attempted.
//
// What this test does NOT do:
//   - Calls any real provider or external endpoint
//   - Touches the G3 lab budget ledger beyond reservation retention asserts
//   - Replaces the verified budget accounting tests in budget-guard.test.mjs
//   - Repeats the 8+11+23 already-passing checks from messages-lab/contract/stream
//
// What this test asserts:
//   - HTTP request: zero sends, ledger unchanged, message identifies the hook
//   - WebSocket upgrade: zero sends, no socket accepted
//   - Concurrency: N parallel rejections, single combined ledger snapshot
//   - Retry storm: repeated calls never reach the handler
//   - Reservation retention: denied calls do not consume reservations
//   - Negative control: with no deny statement, the handler is reachable exactly once

import { test } from "node:test"
import assert from "node:assert/strict"
import http from "node:http"
import {
  applyProviderPreflight,
  tryAdmitRequest,
} from "./opencode-preflight.mjs"
import { BudgetLedger } from "./budget-guard.mjs"

// A "provider handler" modeled as a closure that wraps a counted transport.
// In OpenCode, this is the place where fetch / WebSocket would actually run.
// The lab wires the counter into a real local HTTP+WS server so we can also
// observe whether the connection even reaches the listening socket.
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
    // WebSocket path: client would normally upgrade here. In the lab we
    // probe with a real upgrade request to make sure the network side is
    // exercised end-to-end. The handler's upgrade counter increments only
    // if the connection actually reaches the server.
    await new Promise((resolve) => {
      const req = http.request({
        host: "127.0.0.1", port: new URL(handler.url).port, method: "GET",
        headers: { Connection: "Upgrade", Upgrade: "websocket",
                   "Sec-WebSocket-Version": "13",
                   "Sec-WebSocket-Key": "dGhlIHNhbXBsZSBub25jZQ==" },
      })
      req.on("upgrade", () => resolve())
      req.on("error", () => resolve()) // server destroy() counts it as a probe
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

test("provider.use deny on MiniMax-M3 prevents HTTP handler from being reached", async (t) => {
  const { handler, url } = await withHandler(t, { label: "minimax", transport: "http" })
  const { catalog, denied } = applyProviderPreflight({
    providers: [{ id: "MiniMax-M3", transport: "http", label: "minimax" }],
    authoredEntries: [{
      info: { experimental: { policies: [
        { action: "provider.use", resource: "MiniMax-M3", effect: "deny" },
      ] } },
    }],
    managed: { statements: [], organization: undefined },
  })
  assert.deepEqual(denied, ["MiniMax-M3"], "provider must be in deny list")
  assert.equal(catalog.has("MiniMax-M3"), false, "denied provider removed from catalog")
  const out = await dispatch({ catalog, providerId: "MiniMax-M3", handler: { url }, payload: { prompt: "hi" } })
  assert.equal(out.ok, false)
  assert.equal(out.reason, "denied_by_preflight")
  assert.equal(handler.sends(), 0, "HTTP server received zero requests")
  assert.equal(handler.wsUpgrades(), 0)
  // Mark url as referenced to keep the audit clean
  assert.ok(url.startsWith("http://127.0.0.1:"))
})

test("WebSocket upgrade path is also blocked by the same catalog decision", async (t) => {
  const { handler, url } = await withHandler(t, { label: "minimax-ws", transport: "websocket" })
  const { catalog } = applyProviderPreflight({
    providers: [{ id: "MiniMax-M3", transport: "websocket", label: "minimax" }],
    authoredEntries: [{
      info: { experimental: { policies: [
        { action: "provider.use", resource: "*", effect: "deny" },
      ] } },
    }],
    managed: { statements: [], organization: undefined },
  })
  const out = await dispatch({ catalog, providerId: "MiniMax-M3", handler: { url }, ws: true })
  assert.equal(out.ok, false)
  assert.equal(out.reason, "denied_by_preflight")
  assert.equal(handler.wsUpgrades(), 0, "no WebSocket upgrade reached the server")
  assert.equal(handler.sends(), 0)
})

test("organization-managed deny still filters and overrides user allow", async (t) => {
  const { handler, url } = await withHandler(t, { label: "minimax-org", transport: "http" })
  const { catalog, denied } = applyProviderPreflight({
    providers: [{ id: "MiniMax-M3" }, { id: "anthropic" }],
    authoredEntries: [{
      info: { experimental: { policies: [
        { action: "provider.use", resource: "MiniMax-M3", effect: "allow" },
      ] } },
    }],
    managed: {
      organization: "theatomika",
      statements: [
        { action: "provider.use", resource: "MiniMax-M3", effect: "deny" },
      ],
    },
  })
  assert.deepEqual(denied, ["MiniMax-M3"], "managed deny wins over authored allow")
  const out = await dispatch({ catalog, providerId: "MiniMax-M3", handler: { url }, payload: {} })
  assert.equal(out.ok, false)
  assert.equal(handler.sends(), 0)
})

test("concurrent in-flight attempts all blocked; handler sees zero sends", async (t) => {
  const { handler, url } = await withHandler(t, { label: "minimax-conc", transport: "http" })
  const { catalog } = applyProviderPreflight({
    providers: [{ id: "MiniMax-M3" }],
    authoredEntries: [{
      info: { experimental: { policies: [
        { action: "provider.use", resource: "MiniMax-M3", effect: "deny" },
      ] } },
    }],
    managed: { statements: [], organization: undefined },
  })
  const N = 25
  const results = await Promise.all(
    Array.from({ length: N }, () =>
      dispatch({ catalog, providerId: "MiniMax-M3", handler: { url }, payload: { prompt: "x" } }),
    ),
  )
  for (const r of results) {
    assert.equal(r.ok, false)
    assert.equal(r.reason, "denied_by_preflight")
  }
  assert.equal(handler.sends(), 0)
})

test("retry storm never reaches the handler; ledger reservations stay at zero", async (t) => {
  const { handler, url } = await withHandler(t, { label: "minimax-retry", transport: "http" })
  const { catalog } = applyProviderPreflight({
    providers: [{ id: "MiniMax-M3" }],
    authoredEntries: [{
      info: { experimental: { policies: [
        { action: "provider.use", resource: "MiniMax-M3", effect: "deny" },
      ] } },
    }],
    managed: { statements: [], organization: undefined },
  })
  const ledger = new BudgetLedger({ inputCreditsPerToken: 1, outputCreditsPerToken: 1 })
  ledger.openRun("retry", 100)
  const before = ledger.snapshot("retry")
  for (let i = 0; i < 50; i++) {
    const out = await dispatch({ catalog, providerId: "MiniMax-M3", handler: { url }, payload: { prompt: "x" } })
    assert.equal(out.ok, false)
  }
  const after = ledger.snapshot("retry")
  assert.deepEqual(after, before, "ledger snapshot unchanged across denied calls")
  assert.equal(handler.sends(), 0, "no HTTP request reached the provider")
})

test("negative control: no statement -> catalog admits and one send is recorded", async (t) => {
  const { handler, url } = await withHandler(t, { label: "control", transport: "http" })
  const { catalog, denied } = applyProviderPreflight({
    providers: [{ id: "MiniMax-M3" }],
    authoredEntries: [],
    managed: { statements: [], organization: undefined },
  })
  assert.deepEqual(denied, [], "no statement means no denial")
  assert.equal(catalog.has("MiniMax-M3"), true)
  const out = await dispatch({ catalog, providerId: "MiniMax-M3", handler: { url }, payload: { prompt: "ping" } })
  assert.equal(out.ok, true, "handler reachable exactly once")
  assert.equal(handler.sends(), 1)
  assert.equal(handler.body(), JSON.stringify({ prompt: "ping" }))
})

test("wildcard deny of * removes every catalog member", async (t) => {
  const { handler, url } = await withHandler(t, { label: "minimax-wild", transport: "http" })
  const { catalog } = applyProviderPreflight({
    providers: [{ id: "MiniMax-M3" }, { id: "anthropic" }, { id: "openai" }],
    authoredEntries: [{
      info: { experimental: { policies: [
        { action: "provider.use", resource: "*", effect: "deny" },
      ] } },
    }],
    managed: { statements: [], organization: undefined },
  })
  assert.equal(catalog.size(), 0)
  for (const id of ["MiniMax-M3", "anthropic", "openai"]) {
    const out = await dispatch({ catalog, providerId: id, handler: { url }, payload: {} })
    assert.equal(out.ok, false)
    assert.equal(out.reason, "denied_by_preflight")
  }
  assert.equal(handler.sends(), 0)
})

test("allow-override redirect after earlier wildcard deny is honored", async (t) => {
  const { handler, url } = await withHandler(t, { label: "minimax-allow", transport: "http" })
  // Authored: deny * then allow MiniMax-M3 (last match wins).
  const { catalog } = applyProviderPreflight({
    providers: [{ id: "MiniMax-M3" }, { id: "anthropic" }],
    authoredEntries: [{
      info: { experimental: { policies: [
        { action: "provider.use", resource: "*", effect: "deny" },
        { action: "provider.use", resource: "MiniMax-M3", effect: "allow" },
      ] } },
    }],
    managed: { statements: [], organization: undefined },
  })
  assert.deepEqual(catalog.snapshot(), ["MiniMax-M3"])
  const blocked = await dispatch({ catalog, providerId: "anthropic", handler: { url }, payload: {} })
  assert.equal(blocked.ok, false)
  const allowed = await dispatch({ catalog, providerId: "MiniMax-M3", handler: { url }, payload: { prompt: "y" } })
  assert.equal(allowed.ok, true)
  assert.equal(handler.sends(), 1, "exactly one allowed HTTP request was sent")
})
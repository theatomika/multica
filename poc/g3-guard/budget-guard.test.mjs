import { test } from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { BudgetLedger, createGuardServer } from './budget-guard.mjs';

async function listen(server) {
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  return 'http://127.0.0.1:' + server.address().port;
}

async function harness(t, { credits = 10, mock = async () => ({ result: 'ok', usage: { input_tokens: 2, output_tokens: 1 } }), timeoutMs = 3000 } = {}) {
  let forwarded = 0;
  const provider = http.createServer(async (req, res) => {
    forwarded++;
    let text = '';
    for await (const c of req) text += c;
    const data = JSON.parse(text);
    const response = await mock(data, req);
    if (response === null) { req.socket.destroy(); return; }
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify(response));
  });
  const providerURL = await listen(provider);
  t.after(() => new Promise((resolve) => provider.close(resolve)));
  const ledger = new BudgetLedger({ inputCreditsPerToken: 1, outputCreditsPerToken: 2 });
  ledger.openRun('run-A', credits);
  ledger.openRun('run-B', credits);
  const guard = createGuardServer({ ledger, mockProviderURL: providerURL, maxOutputTokens: 100, timeoutMs });
  const guardURL = await listen(guard);
  t.after(() => new Promise((resolve) => guard.close(resolve)));
  const post = async (data) => {
    const res = await fetch(guardURL + '/v1/run', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ runId: 'run-A', prompt: 'hey', maxOutputTokens: 2, ...data }),
    });
    return { status: res.status, body: await res.json() };
  };
  return { ledger, post, forwarded: () => forwarded };
}

test('successful response settles actual credit usage and releases unused reservation', async (t) => {
  const x = await harness(t);
  assert.deepEqual(await x.post(), { status: 200, body: { result: 'ok', chargedCredits: 4 } });
  assert.deepEqual(x.ledger.snapshot('run-A'), { limit: 10, spent: 4, inFlight: 0, available: 6, accepted: 1, rejected: 0 });
  assert.equal(x.forwarded(), 1);
});

test('preflight rejects over-budget request BEFORE invoking simulated provider', async (t) => {
  const x = await harness(t, { credits: 6 });
  assert.equal((await x.post({ prompt: '0123456789', maxOutputTokens: 2 })).status, 429);
  assert.equal(x.forwarded(), 0);
  assert.equal(x.ledger.snapshot('run-A').spent, 0);
});

test('concurrent requests reserve funds atomically; second cannot oversubscribe in-flight credit', async (t) => {
  let release;
  let entered;
  const started = new Promise((r) => { entered = r; });
  const hold = new Promise((r) => { release = r; });
  const x = await harness(t, { credits: 7, mock: async () => { entered(); await hold; return { result: 'ok', usage: { input_tokens: 2, output_tokens: 1 } }; } });
  const first = x.post();
  await started;
  assert.equal(x.ledger.snapshot('run-A').inFlight, 7);
  assert.equal((await x.post()).status, 429);
  assert.equal(x.forwarded(), 1);
  release();
  assert.equal((await first).status, 200);
  assert.equal(x.ledger.snapshot('run-A').inFlight, 0);
  assert.equal(x.ledger.snapshot('run-A').spent, 4);
});

test('missing usage consumes entire reservation (fail closed) and stops next request', async (t) => {
  const x = await harness(t, { credits: 7, mock: async () => ({ result: 'without usage' }) });
  assert.equal((await x.post()).status, 502);
  assert.equal(x.ledger.snapshot('run-A').spent, 7);
  assert.equal((await x.post()).status, 429);
  assert.equal(x.forwarded(), 1);
});

test('simulated provider overrun above reservation is rejected and fully charged', async (t) => {
  const x = await harness(t, { credits: 7, mock: async () => ({ result: 'untrusted', usage: { input_tokens: 999, output_tokens: 2 } }) });
  assert.equal((await x.post()).status, 502);
  assert.equal(x.ledger.snapshot('run-A').spent, 7);
});

test('transport failure retains worst-case reserved credits; does not retry', async (t) => {
  const x = await harness(t, { credits: 7, mock: async () => null });
  assert.equal((await x.post()).status, 502);
  assert.equal(x.ledger.snapshot('run-A').spent, 7);
  assert.equal(x.forwarded(), 1);
});

test('run budgets remain isolated; missing run is rejected', async (t) => {
  const x = await harness(t);
  assert.equal((await x.post({ runId: 'other-run' })).status, 403);
  assert.equal((await x.post({ runId: 'run-B' })).status, 200);
  assert.equal(x.ledger.snapshot('run-A').spent, 0);
  assert.equal(x.ledger.snapshot('run-B').spent, 4);
});

test('malformed, excessive and fraudulent caller cost fields do not bypass budget', async (t) => {
  const x = await harness(t, { credits: 7 });
  assert.equal((await x.post({ maxOutputTokens: -1 })).status, 400);
  assert.equal((await x.post({ maxOutputTokens: 101 })).status, 400);
  assert.equal((await x.post({ prompt: '' })).status, 400);
  assert.equal((await x.post({ credits: -1000 })).status, 200); // ignored: pricing is only server-side
  assert.equal(x.ledger.snapshot('run-A').spent, 4);
  assert.equal(x.forwarded(), 1);
});

test('ledger rejects invalid rates, budgets and duplicate run IDs before accepting traffic', () => {
  assert.throws(() => new BudgetLedger({ inputCreditsPerToken: 0, outputCreditsPerToken: 1 }));
  const l = new BudgetLedger({ inputCreditsPerToken: 1, outputCreditsPerToken: 1 });
  assert.throws(() => l.openRun('foo', 0));
  l.openRun('foo', 1);
  assert.throws(() => l.openRun('foo', 10));
  assert.throws(() => l.reserve('foo', Number.MAX_SAFE_INTEGER, 99));
  assert.deepEqual(l.snapshot('foo'), { limit: 1, spent: 0, inFlight: 0, available: 1, accepted: 0, rejected: 0 });
});

test('upstream must be loopback, never an unvalidated public host', () => {
  const ledger = new BudgetLedger({ inputCreditsPerToken: 1, outputCreditsPerToken: 1 });
  assert.throws(() => createGuardServer({ ledger, mockProviderURL: 'https://example.com/v1/chat' }));
  assert.throws(() => createGuardServer({ ledger, mockProviderURL: 'http://192.0.2.1/v1/chat' }));
  assert.throws(() => createGuardServer({ ledger, mockProviderURL: 'http://user:pw@127.0.0.1/v1/mock' }));
});

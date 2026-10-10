// G3 laboratory only: no real provider, secrets, authentication, or production integration.
import http from 'node:http';

const isPositiveSafeInteger = (n) => Number.isSafeInteger(n) && n > 0;

export class BudgetLedger {
  #runs = new Map();
  #rates;

  constructor({ inputCreditsPerToken, outputCreditsPerToken }) {
    if (!isPositiveSafeInteger(inputCreditsPerToken) || !isPositiveSafeInteger(outputCreditsPerToken)) {
      throw new TypeError('Token prices must be positive safe integers');
    }
    this.#rates = { input: inputCreditsPerToken, output: outputCreditsPerToken };
  }

  openRun(id, limitCredits) {
    if (typeof id !== 'string' || !/^[a-zA-Z0-9_-]{1,64}$/.test(id)) throw new TypeError('Invalid run ID');
    if (!isPositiveSafeInteger(limitCredits)) throw new TypeError('Invalid run budget');
    if (this.#runs.has(id)) throw new Error('Run already exists');
    this.#runs.set(id, { limit: limitCredits, spent: 0, inFlight: 0, accepted: 0, rejected: 0 });
  }

  hasRun(id) { return this.#runs.has(id); }

  snapshot(id) {
    const run = this.#runs.get(id);
    if (!run) throw new Error('Unknown run');
    return Object.freeze({
      limit: run.limit, spent: run.spent, inFlight: run.inFlight,
      available: run.limit - run.spent - run.inFlight,
      accepted: run.accepted, rejected: run.rejected,
    });
  }

  #credits(inputTokens, outputTokens) {
    if (!isPositiveSafeInteger(inputTokens) || !isPositiveSafeInteger(outputTokens)) {
      throw new TypeError('Token bounds must be positive safe integers');
    }
    const cost = inputTokens * this.#rates.input + outputTokens * this.#rates.output;
    if (!isPositiveSafeInteger(cost)) throw new RangeError('Credit calculation overflow');
    return cost;
  }

  // Synchronous admission: no await between checking funds and reserving them.
  reserve(id, inputTokenUpperBound, outputTokenUpperBound) {
    const run = this.#runs.get(id);
    if (!run) throw new Error('Unknown run');
    const reserveCredits = this.#credits(inputTokenUpperBound, outputTokenUpperBound);
    if (reserveCredits > run.limit - run.spent - run.inFlight) {
      run.rejected++;
      return null;
    }
    run.inFlight += reserveCredits;
    run.accepted++;
    return { id, inputTokenUpperBound, outputTokenUpperBound, reserveCredits, open: true };
  }

  // Missing/untrusted usage or ambiguous network failures retain the FULL reservation.
  chargeFull(ticket) {
    if (!ticket.open) return;
    const run = this.#runs.get(ticket.id);
    run.inFlight -= ticket.reserveCredits;
    run.spent += ticket.reserveCredits;
    ticket.open = false;
  }

  settle(ticket, usage) {
    if (!ticket.open) throw new Error('Reservation already closed');
    if (!usage || !isPositiveSafeInteger(usage.input_tokens) || !Number.isSafeInteger(usage.output_tokens)
        || usage.output_tokens < 0 || usage.input_tokens > ticket.inputTokenUpperBound
        || usage.output_tokens > ticket.outputTokenUpperBound) {
      this.chargeFull(ticket);
      throw new Error('Usage absent or above reserved upper bound');
    }
    const actualCredits = usage.input_tokens * this.#rates.input
      + usage.output_tokens * this.#rates.output;
    if (!Number.isSafeInteger(actualCredits) || actualCredits > ticket.reserveCredits) {
      this.chargeFull(ticket);
      throw new Error('Usage exceeds reservation');
    }
    const run = this.#runs.get(ticket.id);
    run.inFlight -= ticket.reserveCredits;
    run.spent += actualCredits;
    ticket.open = false;
    return actualCredits;
  }
}

function send(res, status, payload) {
  res.writeHead(status, { 'content-type': 'application/json', 'cache-control': 'no-store' });
  res.end(JSON.stringify(payload));
}

async function readSmallJson(req, maxBytes) {
  let bytes = 0;
  const chunks = [];
  for await (const chunk of req) {
    bytes += chunk.length;
    if (bytes > maxBytes) throw new Error('Request too large');
    chunks.push(chunk);
  }
  return JSON.parse(Buffer.concat(chunks).toString('utf8'));
}

// Only an explicitly configured loopback simulator is reachable from this guard.
export function createGuardServer({ ledger, mockProviderURL, maxOutputTokens = 128, timeoutMs = 3000 }) {
  const upstream = new URL(mockProviderURL);
  if (upstream.protocol !== 'http:' || !['127.0.0.1', '[::1]'].includes(upstream.hostname)
      || upstream.username || upstream.password || upstream.search || upstream.hash) {
    throw new Error('Lab guard only accepts an explicit HTTP loopback simulator');
  }
  if (!isPositiveSafeInteger(maxOutputTokens) || !isPositiveSafeInteger(timeoutMs)) throw new TypeError('Invalid limits');

  return http.createServer(async (req, res) => {
    if (req.method !== 'POST' || req.url !== '/v1/run') return send(res, 404, { error: 'not_found' });
    let payload;
    try { payload = await readSmallJson(req, 16384); }
    catch { return send(res, 400, { error: 'invalid_request' }); }
    if (!payload || typeof payload !== 'object' || typeof payload.runId !== 'string'
        || typeof payload.prompt !== 'string' || payload.prompt.length === 0
        || !isPositiveSafeInteger(payload.maxOutputTokens) || payload.maxOutputTokens > maxOutputTokens) {
      return send(res, 400, { error: 'invalid_request' });
    }
    if (!ledger.hasRun(payload.runId)) return send(res, 403, { error: 'unknown_run' });
    // Byte count is a conservative bound only for the controlled simulated provider.
    // It is NOT a verified upper bound for arbitrary real providers or hidden prompts.
    const inputBound = Buffer.byteLength(payload.prompt, 'utf8');
    let ticket;
    try { ticket = ledger.reserve(payload.runId, inputBound, payload.maxOutputTokens); }
    catch { return send(res, 400, { error: 'invalid_budget_bounds' }); }
    if (!ticket) return send(res, 429, { error: 'budget_exhausted' });

    try {
      const providerRes = await fetch(upstream, {
        method: 'POST', headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ prompt: payload.prompt, maxOutputTokens: payload.maxOutputTokens }),
        signal: AbortSignal.timeout(timeoutMs),
      });
      if (!providerRes.ok) throw new Error('Simulated upstream rejected request');
      const raw = await providerRes.text();
      if (Buffer.byteLength(raw, 'utf8') > 32768) throw new Error('Oversized simulated provider response');
      const providerResult = JSON.parse(raw);
      const charged = ledger.settle(ticket, providerResult.usage);
      return send(res, 200, { result: providerResult.result ?? null, chargedCredits: charged });
    } catch {
      ledger.chargeFull(ticket); // No refund without trustworthy usage, even after timeout.
      return send(res, 502, { error: 'upstream_or_usage_unverified' });
    }
  });
}

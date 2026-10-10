// G3 lab only: tests OpenCode v2 MiniMax default Anthropic Messages protocol.
// No provider credentials or remote endpoint; not a production OpenCode proxy.
import http from 'node:http';
const validInt = n => Number.isSafeInteger(n) && n >= 0;
const positive = n => validInt(n) && n > 0;
const fail = (res, code, reason) => { res.writeHead(code, {'content-type':'application/json'});res.end(JSON.stringify({error:reason}));};
function validRequest(r) {
  if (!r || typeof r !== 'object' || Array.isArray(r)) return false;
  const keys = new Set(['model','max_tokens','stream','system','messages','tools']);
  if (!Object.keys(r).every(k => keys.has(k)) || r.model !== 'MiniMax-M3' ||
      r.stream !== true || !positive(r.max_tokens) || r.max_tokens > 16 ||
      !Array.isArray(r.messages) || r.messages.length < 1 || r.messages.length > 12) return false;
  if (r.system !== undefined && (typeof r.system !== 'string' || r.system.length > 2048)) return false;
  if (!r.messages.every(m => m && ['user','assistant'].includes(m.role) &&
       typeof m.content === 'string' && m.content.length > 0 && m.content.length <= 4096)) return false;
  if (r.tools !== undefined && (!Array.isArray(r.tools) || r.tools.length > 4 ||
      !r.tools.every(t => t && typeof t.name === 'string' && t.name.length <= 64 &&
        t.input_schema?.type === 'object'))) return false;
  return true;
}
async function readJson(req) {
  let size = 0; const chunks = [];
  for await (const b of req) { size += b.length; if (size > 16384) throw Error('oversize');chunks.push(b); }
  return JSON.parse(Buffer.concat(chunks).toString('utf8'));
}
async function* events(stream) {
  let buffer = '', size = 0; const decoder = new TextDecoder();
  for await (const chunk of stream) {
    size += chunk.byteLength; if (size > 65536) throw Error('oversize stream');
    buffer = (buffer + decoder.decode(chunk,{stream:true})).replace(/\r\n/g,'\n');
    if (buffer.length > 65536) throw Error('oversize buffer');
    while (buffer.includes('\n\n')) {
      const i = buffer.indexOf('\n\n'), lines = buffer.slice(0,i).split('\n');buffer = buffer.slice(i+2);
      let type='', data='';
      for (const l of lines) {
        if (l.startsWith(':') || l === '') continue;
        if (l.startsWith('event:')) type=l.slice(6).trim();
        else if (l.startsWith('data:')) data+=l.slice(5).trim();
        else throw Error('unsupported SSE');
      }
      if (type && data) yield {type,body:JSON.parse(data)};
    }
  }
  if (decoder.decode() || buffer.trim()) throw Error('incomplete SSE');
}
function checkUsage(start, finish, limit, maxOutput) {
  const s = start?.message?.usage, f = finish?.usage;
  if (!s || !f || !validInt(s.input_tokens) || !validInt(s.cache_read_input_tokens ?? 0) ||
      !validInt(s.cache_creation_input_tokens ?? 0) || !validInt(f.output_tokens)) throw Error('missing usage');
  const totalInput=s.input_tokens+(s.cache_read_input_tokens??0)+(s.cache_creation_input_tokens??0);
  if (!positive(totalInput) || !Number.isSafeInteger(totalInput) || totalInput>limit ||
      f.output_tokens>maxOutput) throw Error('out of bound usage');
  // Lab prices always charge cached tokens at full price, never claim plan-credit parity.
  return {input_tokens:totalInput,output_tokens:f.output_tokens};
}
export function createMiniMaxMessagesLab({ledger,simulatorURL,trustedInputUpperBound,timeoutMs=3000}) {
  const url = new URL(simulatorURL);
  if (url.protocol!=='http:' || url.hostname!=='127.0.0.1' ||
      url.pathname!=='/anthropic/v1/messages' || url.search || url.hash ||
      url.username || url.password) throw Error('mock loopback only');
  if (typeof trustedInputUpperBound!=='function' || !positive(timeoutMs)) throw Error('invalid configuration');
  return http.createServer(async (req,res) => {
    if (req.url!=='/anthropic/v1/messages' || req.method!=='POST') return fail(res,404,'not_found');
    let body;try { body=await readJson(req); } catch { return fail(res,400,'invalid_json'); }
    if (!validRequest(body)) return fail(res,400,'unsupported_request');
    const runId=req.headers['x-g3-lab-run-id'];
    if (typeof runId!=='string' || !ledger.hasRun(runId)) return fail(res,403,'unknown_run');
    let bound;try {bound=await trustedInputUpperBound(body);}catch{return fail(res,503,'unverified_input_bound');}
    if (!positive(bound)) return fail(res,503,'unverified_input_bound');
    let ticket;try {ticket=ledger.reserve(runId,bound,body.max_tokens);}catch{return fail(res,400,'bad_reservation');}
    if (!ticket) return fail(res,429,'budget_exhausted');
    try {
      const upstream=await fetch(url,{method:'POST',headers:{'content-type':'application/json','anthropic-version':'2023-06-01'},
        body:JSON.stringify(body),signal:AbortSignal.timeout(timeoutMs)});
      if (upstream.status!==200 || !upstream.headers.get('content-type')?.includes('text/event-stream')) throw Error('upstream error');
      let start=null,finish=null,stop=false;
      for await (const evt of events(upstream.body)) {
        if (evt.type==='message_start' && evt.body?.type==='message_start' && !start) start=evt.body;
        if (evt.type==='message_delta' && evt.body?.type==='message_delta' && start && !stop) finish=evt.body;
        if (evt.type==='message_stop' && evt.body?.type==='message_stop' && start && finish) {stop=true;break;}
      }
      if (!stop) throw Error('no complete terminal usage');
      const real=checkUsage(start,finish,bound,body.max_tokens);
      const charged=ledger.settle(ticket,real);
      return res.end(JSON.stringify({verified:true,syntheticCredits:charged}));
    }catch {ledger.chargeFull(ticket);return fail(res,502,'unverified_anthropic_stream');}
  });
}

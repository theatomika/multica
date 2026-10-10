import http from 'node:http';
const integer = x => Number.isSafeInteger(x) && x >= 0;
const positive = x => integer(x) && x > 0;
const fields = new Set(['model','messages','max_completion_tokens','stream']);
function reply(res,status,error){res.writeHead(status,{'content-type':'application/json','cache-control':'no-store'});res.end(JSON.stringify({error}));}
async function body(req){let n=0,a=[];for await(const c of req){n+=c.length;if(n>16384)throw Error('too large');a.push(c);}return JSON.parse(Buffer.concat(a).toString('utf8'));}
function valid(b,cap){return b && typeof b==='object' && !Array.isArray(b) &&
 Object.keys(b).every(k=>fields.has(k)) && b.model==='MiniMax-M3' && b.stream===false &&
 positive(b.max_completion_tokens)&&b.max_completion_tokens<=cap &&
 Array.isArray(b.messages)&&b.messages.length>0&&b.messages.length<=24 &&
 b.messages.every(m=>m&&typeof m==='object'&&!Array.isArray(m)&&Object.keys(m).length===2&&
 ['user','assistant','system'].includes(m.role)&&typeof m.content==='string'&&m.content.length>0&&m.content.length<=8192);}
function measured(r,bound,cap){
 if(r?.object!=='chat.completion'||r?.model!=='MiniMax-M3'||r?.base_resp?.status_code!==0||!Array.isArray(r.choices)||!r.choices.length)throw Error('bad response');
 const u=r.usage;
 if(!u||!positive(u.prompt_tokens)||!integer(u.completion_tokens)||!integer(u.total_tokens)||
 u.total_tokens!==u.prompt_tokens+u.completion_tokens||u.prompt_tokens>bound||
 u.completion_tokens>cap||!integer(u.prompt_tokens_details?.cached_tokens??0)||
 (u.prompt_tokens_details?.cached_tokens??0)>u.prompt_tokens)throw Error('untrusted usage');
 // Cache is intentionally charged at full input rate in this lab.
 return {input_tokens:u.prompt_tokens,output_tokens:u.completion_tokens};
}
// Laboratory only: no auth, no production routes, no provider credentials.
export function createMiniMaxMockGate({ledger,mockProviderURL,estimateInputUpperBound,maxCompletionTokens=32,timeoutMs=3000}){
 const target=new URL(mockProviderURL);
 if(target.protocol!=='http:'||target.hostname!=='127.0.0.1'||target.username||target.password||
 target.search||target.hash||target.pathname!=='/v1/chat/completions')throw Error('loopback simulator required');
 if(typeof estimateInputUpperBound!=='function'||!positive(maxCompletionTokens)||!positive(timeoutMs))throw Error('invalid caps');
 return http.createServer(async(req,res)=>{
  if(req.method!=='POST'||req.url!=='/v1/chat/completions')return reply(res,404,'not_found');
  let b;try{b=await body(req);}catch{return reply(res,400,'invalid_json');}
  if(!valid(b,maxCompletionTokens))return reply(res,400,'unsupported_request');
  const id=req.headers['x-g3-lab-run-id'];
  if(typeof id!=='string'||!ledger.hasRun(id))return reply(res,403,'unknown_run');
  let bound;try{bound=await estimateInputUpperBound(b);}catch{return reply(res,503,'input_bound_unavailable');}
  if(!positive(bound))return reply(res,503,'input_bound_unavailable');
  let ticket;try{ticket=ledger.reserve(id,bound,b.max_completion_tokens);}catch{return reply(res,400,'invalid_reservation');}
  if(!ticket)return reply(res,429,'budget_exhausted');
  try{
   const u=await fetch(target,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(b),signal:AbortSignal.timeout(timeoutMs)});
   if(u.status!==200)throw Error('mock upstream status');
   const raw=await u.text();if(Buffer.byteLength(raw)>32768)throw Error('oversize');
   const completion=JSON.parse(raw);
   const usage=measured(completion,bound,b.max_completion_tokens);
   const charged=ledger.settle(ticket,usage);
   res.writeHead(200,{'content-type':'application/json','cache-control':'no-store'});
   return res.end(JSON.stringify({chargedSyntheticCredits:charged,completion}));
  }catch{
   ledger.chargeFull(ticket);
   return reply(res,502,'provider_usage_unverified');
  }
 });
}

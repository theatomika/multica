import{test}from'node:test';import assert from'node:assert/strict';import http from'node:http';
import{BudgetLedger}from'./budget-guard.mjs';import{createMiniMaxStreamLab}from'./minimax-stream.mjs';
const body={model:'MiniMax-M3',messages:[{role:'user',content:'hello'}],stream:true,max_completion_tokens:6};
const delta={object:'chat.completion.chunk',model:'MiniMax-M3',choices:[{index:0,delta:{content:'hi'}}]};
const usage={object:'chat.completion.chunk',model:'MiniMax-M3',choices:[],usage:{prompt_tokens:4,completion_tokens:3,total_tokens:7,prompt_tokens_details:{cached_tokens:2}}};
const ev=x=>'data: '+(typeof x==='string'?x:JSON.stringify(x))+'\n\n';
async function listen(server){await new Promise((ok,bad)=>{server.once('error',bad);server.listen(0,'127.0.0.1',ok)});return'http://127.0.0.1:'+server.address().port}
async function setup(t,{credit=22,bound=()=>10,provider=async r=>r.end(ev(delta)+ev(usage)+ev('[DONE]'))}={}){
 let calls=0,seen;
 const fake=http.createServer(async(req,res)=>{calls++;let str='';for await(const c of req)str+=c;seen=JSON.parse(str);
   res.writeHead(200,{'content-type':'text/event-stream'});await provider(res);});
 const base=await listen(fake);t.after(()=>new Promise(r=>fake.close(r)));
 const ledger=new BudgetLedger({inputCreditsPerToken:1,outputCreditsPerToken:2});
 ledger.openRun('a',credit);ledger.openRun('b',credit);
 const guard=createMiniMaxStreamLab({ledger,providerURL:base+'/v1/chat/completions',trustedInputUpperBound:bound});
 const url=await listen(guard);t.after(()=>new Promise(r=>guard.close(r)));
 const post=async(b=body,id='a')=>{const r=await fetch(url+'/v1/chat/completions',{method:'POST',headers:{'content-type':'application/json','x-g3-lab-run-id':id},body:JSON.stringify(b)});return{status:r.status,text:await r.text()};};
 return{post,calls:()=>calls,seen:()=>seen,ledger};
}
test('real SSE deltas forward before final usage and DONE; settle credits',async t=>{
 const x=await setup(t),r=await x.post();assert.equal(r.status,200);assert.match(r.text,/hi/);assert.match(r.text,/data: \[DONE\]/);
 assert.equal(x.ledger.snapshot('a').spent,10);assert.equal(x.ledger.snapshot('a').inFlight,0);
 assert.deepEqual(x.seen().stream_options,{include_usage:true});
});
test('before dispatch rejects insufficient budget',async t=>{const x=await setup(t,{credit:21});assert.equal((await x.post()).status,429);assert.equal(x.calls(),0);});
test('unverified input count fails closed',async t=>{const x=await setup(t,{bound:()=>undefined});assert.equal((await x.post()).status,503);assert.equal(x.calls(),0);});
test('fake client budget, tools and nonstreaming are rejected',async t=>{
 const x=await setup(t);for(const v of [{input_upper_bound:1},{tools:[]},{stream:false}])assert.equal((await x.post({...body,...v})).status,400);
 assert.equal(x.calls(),0);
});
test('missing terminal usage charges full and signals failure',async t=>{
 const x=await setup(t,{provider:async r=>r.end(ev(delta)+ev('[DONE]'))});
 const r=await x.post();assert.match(r.text,/g3_error/);assert.doesNotMatch(r.text,/data: \[DONE\]/);
 assert.equal(x.ledger.snapshot('a').spent,22);
});
test('overrun in terminal usage retains full reservation',async t=>{
 const bad={...usage,usage:{...usage.usage,prompt_tokens:99,total_tokens:102}};
 const x=await setup(t,{provider:async r=>r.end(ev(delta)+ev(bad)+ev('[DONE]'))});
 assert.match((await x.post()).text,/g3_error/);assert.equal(x.ledger.snapshot('a').spent,22);
});
test('fragmented CRLF frames and comments accepted',async t=>{
 const x=await setup(t,{provider:async r=>{r.write(': ping\r\n\r\n');const str=(ev(delta)+ev(usage)+ev('[DONE]')).replaceAll('\n','\r\n');for(const ch of str.slice(0,11))r.write(ch);r.end(str.slice(11));}});
 assert.match((await x.post()).text,/data: \[DONE\]/);assert.equal(x.ledger.snapshot('a').spent,10);
});
test('atomic in-flight reservation rejects second concurrent stream',async t=>{
 let enter,unblock;const started=new Promise(r=>enter=r),waiting=new Promise(r=>unblock=r);
 const x=await setup(t,{provider:async r=>{r.write(ev(delta));enter();await waiting;r.end(ev(usage)+ev('[DONE]'));}});
 const first=x.post();await started;assert.equal(x.ledger.snapshot('a').inFlight,22);assert.equal((await x.post()).status,429);
 unblock();assert.match((await first).text,/data: \[DONE\]/);assert.equal(x.calls(),1);
});
test('network break after delta charges full and does not retry',async t=>{
 const x=await setup(t,{provider:async r=>{r.write(ev(delta));r.socket.destroy();}});
 const result=await x.post();assert.match(result.text,/(?:g3_error|stream_not_verified)/);assert.equal(x.ledger.snapshot('a').spent,22);assert.equal(x.calls(),1);
});
test('run isolation and unknown ID',async t=>{
 const x=await setup(t);assert.equal((await x.post(body,'missing')).status,403);assert.equal((await x.post(body,'b')).status,200);
 assert.equal(x.ledger.snapshot('a').spent,0);assert.equal(x.ledger.snapshot('b').spent,10);
});
test('rejects any non-loopback or wrong path upstream',()=>{
 const ledger=new BudgetLedger({inputCreditsPerToken:1,outputCreditsPerToken:1});
 for(const url of ['https://api.minimax.io/v1/chat/completions','http://192.0.2.1/v1/chat/completions','http://127.0.0.1:80/v1/responses'])
 assert.throws(()=>createMiniMaxStreamLab({ledger,providerURL:url,trustedInputUpperBound:()=>10}));
});

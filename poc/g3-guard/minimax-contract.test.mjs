import {test} from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import {BudgetLedger} from './budget-guard.mjs';
import {createMiniMaxMockGate} from './minimax-contract.mjs';
const request={model:'MiniMax-M3',messages:[{role:'user',content:'test'}],max_completion_tokens:6,stream:false};
const completion={id:'mock',object:'chat.completion',model:'MiniMax-M3',choices:[{index:0,message:{role:'assistant',content:'ok'},finish_reason:'stop'}],base_resp:{status_code:0},usage:{prompt_tokens:4,completion_tokens:3,total_tokens:7,prompt_tokens_details:{cached_tokens:1}}};
async function listen(server){await new Promise((r,j)=>{server.once('error',j);server.listen(0,'127.0.0.1',r)});return 'http://127.0.0.1:'+server.address().port}
async function setup(t,{limit=22,mock=async()=>completion,estimate=async()=>10}={}){
 let calls=0;
 const provider=http.createServer(async(req,res)=>{calls++;for await(const chunk of req)void chunk;const result=await mock();if(result===null){req.socket.destroy();return;}res.writeHead(200,{'content-type':'application/json'});res.end(JSON.stringify(result));});
 const origin=await listen(provider);t.after(()=>new Promise(r=>provider.close(r)));
 const ledger=new BudgetLedger({inputCreditsPerToken:1,outputCreditsPerToken:2});
 ledger.openRun('test-run',limit);ledger.openRun('other-run',limit);
 const gate=createMiniMaxMockGate({ledger,mockProviderURL:origin+'/v1/chat/completions',estimateInputUpperBound:estimate});
 const url=await listen(gate);t.after(()=>new Promise(r=>gate.close(r)));
 const post=async(data=request,id='test-run')=>{const r=await fetch(url+'/v1/chat/completions',{method:'POST',headers:{'content-type':'application/json','x-g3-lab-run-id':id},body:JSON.stringify(data)});return {status:r.status,body:await r.json()};};
 return {post,ledger,calls:()=>calls};
}
test('usage including cached tokens settles conservatively',async t=>{
 const f=await setup(t),r=await f.post();assert.equal(r.status,200);assert.equal(r.body.chargedSyntheticCredits,10);
 assert.deepEqual(f.ledger.snapshot('test-run'),{limit:22,spent:10,inFlight:0,available:12,accepted:1,rejected:0});
});
test('reservation rejects before mock upstream',async t=>{const f=await setup(t,{limit:21});assert.equal((await f.post()).status,429);assert.equal(f.calls(),0);});
test('streaming never forwarded',async t=>{const f=await setup(t);assert.equal((await f.post({...request,stream:true})).status,400);assert.equal(f.calls(),0);});
test('model, output cap, tools, priority and legacy max_tokens rejected',async t=>{
 const f=await setup(t);
 for(const x of [{model:'MiniMax-M2.7'},{max_completion_tokens:100},{tools:[]},{service_tier:'priority'},{max_tokens:100000}])assert.equal((await f.post({...request,...x})).status,400);
 assert.equal(f.calls(),0);
});
test('no attested input bound -> fail closed',async t=>{const f=await setup(t,{estimate:async()=>undefined});assert.equal((await f.post()).status,503);assert.equal(f.calls(),0);});
test('overrun of input bound consumes full reservation',async t=>{
 const f=await setup(t,{mock:async()=>({...completion,usage:{...completion.usage,prompt_tokens:100,total_tokens:103}})});
 assert.equal((await f.post()).status,502);assert.equal(f.ledger.snapshot('test-run').spent,22);
});
test('missing usage consumes full reservation',async t=>{const f=await setup(t,{mock:async()=>({...completion,usage:undefined})});assert.equal((await f.post()).status,502);assert.equal(f.ledger.snapshot('test-run').spent,22);});
test('fake cache count over input rejected',async t=>{const f=await setup(t,{mock:async()=>({...completion,usage:{...completion.usage,prompt_tokens_details:{cached_tokens:999}}})});assert.equal((await f.post()).status,502);});
test('mismatched model, total count, provider error rejected',async t=>{
 for(const result of [{...completion,model:'other'},{...completion,usage:{...completion.usage,total_tokens:1000}},{...completion,base_resp:{status_code:1004}}]){
  const f=await setup(t,{mock:async()=>result});assert.equal((await f.post()).status,502);assert.equal(f.ledger.snapshot('test-run').spent,22);
 }
});
test('transport failure never retries and keeps full reserve',async t=>{const f=await setup(t,{mock:async()=>null});assert.equal((await f.post()).status,502);assert.equal(f.calls(),1);assert.equal(f.ledger.snapshot('test-run').spent,22);});
test('unknown run rejected; other run stays isolated',async t=>{const f=await setup(t);assert.equal((await f.post(request,'unknown')).status,403);assert.equal((await f.post(request,'other-run')).status,200);assert.equal(f.ledger.snapshot('test-run').spent,0);assert.equal(f.ledger.snapshot('other-run').spent,10);});
test('concurrent requests cannot double-spend in-flight reservation',async t=>{
 let unblock,signal;const wait=new Promise(r=>unblock=r),started=new Promise(r=>signal=r);
 const f=await setup(t,{mock:async()=>{signal();await wait;return completion;}});
 const first=f.post();await started;assert.equal(f.ledger.snapshot('test-run').inFlight,22);
 assert.equal((await f.post()).status,429);assert.equal(f.calls(),1);unblock();assert.equal((await first).status,200);
});
test('actual MiniMax API URL is never accepted in this laboratory',()=>{
 const ledger=new BudgetLedger({inputCreditsPerToken:1,outputCreditsPerToken:1});
 for(const u of ['https://api.minimax.io/v1/chat/completions','http://localhost:1234/v1/chat/completions','http://127.0.0.1:1234/other','http://me:pw@127.0.0.1:1234/v1/chat/completions'])
  assert.throws(()=>createMiniMaxMockGate({ledger,mockProviderURL:u,estimateInputUpperBound:()=>10}));
});

import {test} from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import {BudgetLedger} from './budget-guard.mjs';
import {createMiniMaxMessagesLab} from './minimax-messages-lab.mjs';
const request={model:'MiniMax-M3',max_tokens:6,stream:true,system:'coding',messages:[{role:'user',content:'test'}]};
const ev=(type,payload)=>'event: '+type+'\ndata: '+JSON.stringify(payload)+'\n\n';
const start=ev('message_start',{type:'message_start',message:{usage:{input_tokens:2,cache_read_input_tokens:1,cache_creation_input_tokens:1}}});
const delta=ev('message_delta',{type:'message_delta',usage:{output_tokens:3}});
const stop=ev('message_stop',{type:'message_stop'});
async function listen(server){await new Promise((ok,no)=>{server.once('error',no);server.listen(0,'127.0.0.1',ok);});return'http://127.0.0.1:'+server.address().port;}
async function harness(t,{budget=22,provider=async r=>r.end(start+delta+stop),bound=async()=>10}={}){
 let count=0,wire;
 const sim=http.createServer(async(req,res)=>{count++;let data='';for await(const c of req)data+=c;wire=JSON.parse(data);
   res.writeHead(200,{'content-type':'text/event-stream'});await provider(res);});
 const base=await listen(sim);t.after(()=>new Promise(ok=>sim.close(ok)));
 const ledger=new BudgetLedger({inputCreditsPerToken:1,outputCreditsPerToken:2});
 ledger.openRun('one',budget);ledger.openRun('two',budget);
 const gate=createMiniMaxMessagesLab({ledger,simulatorURL:base+'/anthropic/v1/messages',trustedInputUpperBound:bound});
 const url=await listen(gate);t.after(()=>new Promise(ok=>gate.close(ok)));
 const post=async(r=request,id='one')=>{const response=await fetch(url+'/anthropic/v1/messages',{method:'POST',headers:{'content-type':'application/json','x-g3-lab-run-id':id},body:JSON.stringify(r)});return{status:response.status,body:await response.json()};};
 return{post,ledger,count:()=>count,wire:()=>wire};
}
test('Anthropic Messages counts input plus both caches and output only after message_stop',async t=>{
 const x=await harness(t),r=await x.post();assert.equal(r.status,200);assert.equal(r.body.syntheticCredits,10);
 assert.equal(x.ledger.snapshot('one').spent,10);assert.equal(x.ledger.snapshot('one').inFlight,0);assert.equal(x.wire().max_tokens,6);
});
test('preflight rejects before network when reserved budget exceeds available',async t=>{
 const x=await harness(t,{budget:21});assert.equal((await x.post()).status,429);assert.equal(x.count(),0);
});
test('no provable input upper bound always fails closed',async t=>{
 const x=await harness(t,{bound:async()=>undefined});assert.equal((await x.post()).status,503);assert.equal(x.count(),0);
});
test('tool schema allowed in mock Anthropic payload; unsupported fields rejected',async t=>{
 const x=await harness(t);
 const r=await x.post({...request,tools:[{name:'lookup',description:'read',input_schema:{type:'object'}}]});
 assert.equal(r.status,200);assert.equal(x.wire().tools[0].name,'lookup');
 assert.equal((await x.post({...request,max_completion_tokens:9999})).status,400);
});
test('missing terminal message_stop retains full reservation',async t=>{
 const x=await harness(t,{provider:async r=>r.end(start+delta)});assert.equal((await x.post()).status,502);
 assert.equal(x.ledger.snapshot('one').spent,22);
});
test('cached input exceeding reserved bound retains full reservation',async t=>{
 const over=ev('message_start',{type:'message_start',message:{usage:{input_tokens:2,cache_read_input_tokens:99,cache_creation_input_tokens:1}}});
 const x=await harness(t,{provider:async r=>r.end(over+delta+stop)});assert.equal((await x.post()).status,502);
 assert.equal(x.ledger.snapshot('one').spent,22);
});
test('transport disconnection retains reservation, never retries',async t=>{
 const x=await harness(t,{provider:async r=>{r.write(start);r.socket.destroy();}});
 assert.equal((await x.post()).status,502);assert.equal(x.count(),1);assert.equal(x.ledger.snapshot('one').spent,22);
});
test('run accounting is isolated and real provider urls are disallowed',async t=>{
 const x=await harness(t);assert.equal((await x.post(request,'other')).status,403);
 assert.equal((await x.post(request,'two')).status,200);assert.equal(x.ledger.snapshot('one').spent,0);
 const ledger=new BudgetLedger({inputCreditsPerToken:1,outputCreditsPerToken:2});
 assert.throws(()=>createMiniMaxMessagesLab({ledger,simulatorURL:'https://api.minimax.io/anthropic/v1/messages',trustedInputUpperBound:()=>10}));
});

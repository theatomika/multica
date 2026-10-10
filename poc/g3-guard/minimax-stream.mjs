// G3 mock-only SSE gate. No real provider, credentials, or Lunar access.
import http from 'node:http';
const integer=n=>Number.isSafeInteger(n)&&n>=0, pos=n=>integer(n)&&n>0;
function response(res,status,error){
  if(!res.headersSent)res.writeHead(status,{'content-type':'application/json','cache-control':'no-store'});
  if(!res.destroyed&&!res.writableEnded)res.end(JSON.stringify({error}));
}
async function readBody(req){
  let n=0,parts=[];
  for await(const c of req){n+=c.length;if(n>16384)throw Error('oversize');parts.push(c);}
  return JSON.parse(Buffer.concat(parts).toString('utf8'));
}
function valid(b,cap){
  if(!b||Array.isArray(b)||typeof b!=='object')return false;
  const keys=new Set(['model','messages','stream','stream_options','max_completion_tokens']);
  return Object.keys(b).every(k=>keys.has(k))&&b.model==='MiniMax-M3'&&b.stream===true&&
    pos(b.max_completion_tokens)&&b.max_completion_tokens<=cap&&
    (b.stream_options===undefined || (b.stream_options?.include_usage===true&&Object.keys(b.stream_options).length===1))&&
    Array.isArray(b.messages)&&b.messages.length>0&&b.messages.length<=24&&
    b.messages.every(m=>m&&typeof m==='object'&&!Array.isArray(m)&&Object.keys(m).length===2&&
      ['user','assistant','system'].includes(m.role)&&typeof m.content==='string'&&m.content.length>0&&m.content.length<=8192);
}
function trustedUsage(u,input,maxOutput){
  if(!u||!pos(u.prompt_tokens)||!integer(u.completion_tokens)||!integer(u.total_tokens)||
     u.total_tokens!==u.prompt_tokens+u.completion_tokens||
     u.prompt_tokens>input||u.completion_tokens>maxOutput||
     !integer(u.prompt_tokens_details?.cached_tokens??0)||
     (u.prompt_tokens_details?.cached_tokens??0)>u.prompt_tokens)throw Error('unverifiable usage');
  return {input_tokens:u.prompt_tokens,output_tokens:u.completion_tokens};
}
async function* frames(stream){
  const decoder=new TextDecoder('utf-8',{fatal:true});let buffer='',bytes=0,count=0;
  for await(const chunk of stream){
    bytes+=chunk.byteLength;if(bytes>65536)throw Error('too many stream bytes');
    buffer=(buffer+decoder.decode(chunk,{stream:true})).replace(/\r\n/g,'\n');
    if(buffer.length>65536)throw Error('buffer too large');
    while(buffer.includes('\n\n')){
      const i=buffer.indexOf('\n\n'),frame=buffer.slice(0,i);buffer=buffer.slice(i+2);
      const data=[];
      for(const line of frame.split('\n')){
        if(!line||line.startsWith(':'))continue;
        if(!line.startsWith('data:'))throw Error('unsupported SSE metadata');
        data.push(line.slice(5).trimStart());
      }
      if(!data.length)continue;
      if(++count>128)throw Error('too many SSE events');
      yield data.join('\n');
    }
  }
  if(decoder.decode()||buffer.trim())throw Error('truncated SSE');
}
export function createMiniMaxStreamLab({ledger,providerURL,trustedInputUpperBound,maxOutputTokens=16,timeoutMs=3000}){
 const url=new URL(providerURL);
 if(url.protocol!=='http:'||url.hostname!=='127.0.0.1'||url.pathname!=='/v1/chat/completions'||
    url.username||url.password||url.search||url.hash)throw Error('loopback mock only');
 if(typeof trustedInputUpperBound!=='function'||!pos(maxOutputTokens)||!pos(timeoutMs))throw Error('bad settings');
 return http.createServer(async(req,res)=>{
   if(req.method!=='POST'||req.url!=='/v1/chat/completions')return response(res,404,'not_found');
   let b;try{b=await readBody(req);}catch{return response(res,400,'bad_json');}
   if(!valid(b,maxOutputTokens))return response(res,400,'unsupported_request');
   const runId=req.headers['x-g3-lab-run-id'];
   if(typeof runId!=='string'||!ledger.hasRun(runId))return response(res,403,'unknown_run');
   let bound;try{bound=await trustedInputUpperBound(b);}catch{return response(res,503,'no_verified_input_bound');}
   if(!pos(bound))return response(res,503,'no_verified_input_bound');
   let ticket;try{ticket=ledger.reserve(runId,bound,b.max_completion_tokens);}
   catch{return response(res,400,'invalid_budget');}
   if(!ticket)return response(res,429,'budget_exhausted');
   const abort=new AbortController(),disconnect=()=>abort.abort();
   res.once('close',disconnect);
   let settled=false,seenDelta=false,finalUsage=null,ended=false;
   try{
     const upstream=await fetch(url,{method:'POST',headers:{'content-type':'application/json'},
       body:JSON.stringify({...b,stream_options:{include_usage:true}}),
       signal:AbortSignal.any([abort.signal,AbortSignal.timeout(timeoutMs)])});
     if(upstream.status!==200||!upstream.headers.get('content-type')?.includes('text/event-stream'))throw Error('bad upstream');
     for await(const data of frames(upstream.body)){
       if(data==='[DONE]'){if(!seenDelta||!finalUsage)throw Error('missing usage');ended=true;break;}
       if(ended)throw Error('frame after done');
       const packet=JSON.parse(data);
       if(packet.object!=='chat.completion.chunk'||packet.model!=='MiniMax-M3')throw Error('bad chunk');
       if(packet.usage!==undefined&&packet.usage!==null){
         if(finalUsage||(Array.isArray(packet.choices)&&packet.choices.length))throw Error('bad terminal usage');
         finalUsage=trustedUsage(packet.usage,bound,b.max_completion_tokens);
       }else{
         if(finalUsage||!Array.isArray(packet.choices)||!packet.choices.length)throw Error('bad delta');
         seenDelta=true;
         if(!res.headersSent)res.writeHead(200,{'content-type':'text/event-stream','cache-control':'no-store'});
         if(!res.write('data: '+JSON.stringify(packet)+'\n\n')){
           await new Promise((ok,no)=>{res.once('drain',ok);res.once('close',()=>no(Error('client disconnected')));});
         }
       }
     }
     if(!ended||!finalUsage)throw Error('incomplete stream');
     ledger.settle(ticket,finalUsage);settled=true;
     if(!res.headersSent)res.writeHead(200,{'content-type':'text/event-stream'});
     res.end('data: '+JSON.stringify({object:'chat.completion.chunk',model:'MiniMax-M3',choices:[],usage:{
       prompt_tokens:finalUsage.input_tokens,completion_tokens:finalUsage.output_tokens,
       total_tokens:finalUsage.input_tokens+finalUsage.output_tokens}})+'\n\ndata: [DONE]\n\n');
   }catch{
     if(!settled)ledger.chargeFull(ticket);
     if(res.headersSent){if(!res.destroyed)res.end('event: g3_error\ndata: {"error":"stream_not_verified"}\n\n');}
     else response(res,502,'stream_not_verified');
   }finally{res.off('close',disconnect);abort.abort();}
 });
}

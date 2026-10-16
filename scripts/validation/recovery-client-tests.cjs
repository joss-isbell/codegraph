// Real owned children exercise the same transport used by the B rehearsal.
const fs = require('node:fs');
const path = require('node:path');
const { once } = require('node:events');
const { Client } = require('./recovery-client.cjs');
const out = process.env.VALIDATION_OUT;
const scenario = 'recovery-nested-output';
const results = [];
const emit = (type, fields={}) => fs.appendFileSync(process.env.VALIDATION_EVENTS,JSON.stringify({type,atEpochSeconds:Date.now()/1000,...fields})+'\n');
const check = (name, passed, evidence={}) => {results.push(!!passed);emit('assert',{scenario,name,passed:!!passed,...evidence});};
const size = name => fs.existsSync(path.join(out,name)) ? fs.statSync(path.join(out,name)).size : -1;
const read = name => fs.existsSync(path.join(out,name)) ? fs.readFileSync(path.join(out,name),'utf8') : '';
const delay = ms => new Promise(resolve=>setTimeout(resolve,ms));
const all = [];
function client(name, script, settings={}) {
  const cleanup=[];
  const value=new Client(out,name,{out,cleanup,launch:{command:process.execPath,args:['-e',script],env:{}},
    stdoutLimit:1024,stderrLimit:1024,requestDeadline:1000,closeDeadline:1000,...settings});
  all.push(value);return value;
}
async function rejected(operation) {try{await operation();return '';}catch(error){return String(error);}}
async function main() {
  emit('start',{scenario});
  for(const mode of ['nonzero','signal']) {
    const value=client(mode,`process.stderr.write('retained-${mode}-diagnostic\\n',()=>${mode==='nonzero'?'process.exit(23)':'process.kill(process.pid,"SIGTERM")'});`);
    const exit=once(value.child,'exit');await exit;await delay(10);
    const error=await rejected(()=>value.close());
    check('failure diagnostics retained for '+mode,error.includes('MCP cleanup exit')&&read(mode+'.stderr.log').includes('retained-'+mode+'-diagnostic'),{error,retainedBytes:size(mode+'.stderr.log')});
  }
  const slow=client('timeout',"process.stderr.write('retained-timeout-diagnostic\\n');process.stdin.resume();process.stdin.on('end',()=>setTimeout(()=>process.exit(0),200));",{closeDeadline:20});
  await delay(100);
  const timeout=await rejected(()=>slow.close());
  check('graceful-close deadline retains diagnostics before child later exits',timeout.includes('graceful-close deadline')&&read('timeout.stderr.log').includes('retained-timeout-diagnostic'),{timeout});
  await once(slow.child,'exit');await slow.close();
  for(const stream of ['stdout','stderr']) {
    const noisy=client(stream,`let count=0;const timer=setInterval(()=>{process.${stream}.write(${stream==='stdout'?'JSON.stringify({jsonrpc:"2.0",method:"noise",params:"x".repeat(30)})+"\\n"':'"bounded-error-"+"x".repeat(50)+"\\n"'});if(++count===80)clearInterval(timer);},1);process.stdin.resume();process.stdin.on('end',()=>{clearInterval(timer);process.exit(0);});`);
    const failure=await rejected(()=>noisy.request('never-responds'));
    const closed=await rejected(()=>noisy.close());
    const file=stream==='stdout'?stream+'.rpc.jsonl':stream+'.stderr.log';
    check('cumulative '+stream+' overflow fails even for short complete lines',failure.includes('MCP '+stream+' overflow'),{failure,closed});
    check(stream+' diagnostic retention stays within total byte cap',size(file)>=0&&size(file)<=1024,{retainedBytes:size(file),cap:1024});
  }
  check('every fixture child has exited before the complete test tail',all.every(value=>value.child.exitCode!==null||value.child.signalCode!==null));
  emit('complete',{scenario});emit('final',{passed:results.every(Boolean),scenarios:[scenario]});
  if(!results.every(Boolean))process.exitCode=1;
}
main().catch(error=>{console.error(error.stack);process.exitCode=1;}).finally(async()=>{
  for(const value of all)if(value.child.exitCode===null&&value.child.signalCode===null){value.child.stdin.end();await once(value.child,'exit');}
  fs.writeFileSync(path.join(out,'nested-owned-cleanup.json'),JSON.stringify({complete:all.every(value=>value.child.exitCode!==null||value.child.signalCode!==null),children:all.map(value=>({name:value.name,pid:value.child.pid,exitCode:value.child.exitCode,signal:value.child.signalCode}))},null,2)+'\n');
});

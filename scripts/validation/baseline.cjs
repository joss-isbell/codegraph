// Baseline/candidate observation adapter; all project roots belong to this attempt.
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { spawn, spawnSync } = require('node:child_process');
const { pathToFileURL } = require('node:url');
const { DatabaseSync } = require('node:sqlite');
const out = process.env.VALIDATION_OUT;
// The fixture's global Git configuration is the runner-owned empty sandbox.
// Exclude host system configuration from both Git and the engine's Git probes.
process.env.GIT_CONFIG_NOSYSTEM = '1';
const artifact = path.resolve(process.env.BASELINE_ROOT);
const { default: CodeGraph } = require(path.join(artifact, 'dist'));
const { scanDirectory } = require(path.join(artifact, 'dist/extraction'));
const bin = path.join(artifact, 'dist/bin/codegraph.js');
const flags = ['--liftoff-only', '--disable-warning=ExperimentalWarning'];
const scenarios = ['discovery-index', 'shared-sessions-projects', 'catchup-query', 'idle-resources', 'direct-session'];
const emit = (type, fields = {}) => fs.appendFileSync(process.env.VALIDATION_EVENTS, JSON.stringify({ type, atEpochSeconds: Date.now()/1000, ...fields }) + '\n');
const check = (scenario, name, passed, evidence = {}) => {
  emit('assert', { scenario, name, passed: !!passed, ...evidence });
  if (!passed) throw new Error(name);
};
const write = (name, value) => fs.writeFileSync(path.join(out, name), JSON.stringify(value, null, 2) + '\n');
const hash = data => crypto.createHash('sha256').update(data).digest('hex');
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
const timings = [];
const projectFacts = [];
const sessionFacts = [];
const clients = [];
let callId = 1;
let phase = null;
const sampleIndex = () => fs.existsSync(path.join(out,'resources.jsonl')) ? fs.readFileSync(path.join(out,'resources.jsonl'),'utf8').split('\n').filter(Boolean).length : 0;
const phaseStart = name => { phase = name; emit('start', { scenario: name }); fs.appendFileSync(path.join(out,'phases.jsonl'), JSON.stringify({phase:name,atEpochMs:Date.now(),resourceSampleIndex:sampleIndex(),type:'start'})+'\n'); };
const phaseEnd = () => { emit('complete', {scenario:phase}); fs.appendFileSync(path.join(out,'phases.jsonl'),JSON.stringify({phase,atEpochMs:Date.now(),resourceSampleIndex:sampleIndex(),type:'complete'})+'\n'); };

function semantic(root) {
  const db = new DatabaseSync(path.join(root,'.codegraph/codegraph.db'), { readOnly:true });
  try {
    db.exec('BEGIN');
    const files = db.prepare('SELECT path,content_hash,language,size,generated FROM files ORDER BY path').all();
    const rows = db.prepare('SELECT id,kind,name,qualified_name,file_path,start_line,end_line FROM nodes ORDER BY file_path,start_line,kind,name').all();
    const key = node => [node.file_path,node.kind,node.qualified_name,node.name,node.start_line,node.end_line].join(':');
    const ids = new Map(rows.map(node => [node.id,key(node)]));
    const nodes = rows.map(({id,...node}) => node);
    const edges = db.prepare('SELECT source,target,kind,metadata,line,col,provenance FROM edges').all().map(({source,target,...edge}) => ({source:ids.get(source),target:ids.get(target),...edge})).sort((a,b)=>JSON.stringify(a).localeCompare(JSON.stringify(b)));
    db.exec('COMMIT');
    return {files,nodes,edges};
  } finally {db.close();}
}
function fixtureInputs(root) {
  const files = ['.gitignore','.git/info/exclude','.git/config','codegraph.json','src/app.ts','src/service.ts','src/leaf.ts'];
  const configured = spawnSync('git',['-C',root,'config','--get','core.excludesFile'],{encoding:'utf8',windowsHide:true});
  const global = process.env.GIT_CONFIG_GLOBAL;
  const selected = files.map(file=>({path:file,absolute:path.join(root,file)}));
  if(global) selected.push({path:'sandbox-global-git-config',absolute:global});
  selected.push({path:'sandbox-default-global-ignore',absolute:path.join(process.env.XDG_CONFIG_HOME,'git','ignore')});
  if(configured.status===0&&configured.stdout.trim()) {
    const file=configured.stdout.trim().replace(/^~(?=[\\/]|$)/,process.env.HOME);
    selected.push({path:'configured-core-excludesFile',absolute:path.resolve(root,file)});
  }
  return {gitConfiguration:{systemDisabled:true,globalConfigSelected:!!global,coreExcludesFile:configured.stdout.trim()||null},files:selected.map(({path:file,absolute})=>{
    if(!fs.existsSync(absolute)) return {path:file,exists:false};
    const data=fs.readFileSync(absolute);
    return {path:file,exists:true,bytes:data.length,sha256:hash(data),utf8:data.toString('utf8')};
  })};
}
function daemonPid(root) {
  try {return JSON.parse(fs.readFileSync(path.join(root,'.codegraph/daemon.pid'),'utf8')).pid;} catch{return null;}
}
function alive(pid) {try{process.kill(pid,0);return true;}catch{return false;}}
class Client {
  constructor(root, name, direct=false) {
    this.name = name;
    this.pending = new Map();
    this.buffer = '';
    this.stderr = '';
    const argv = [...flags, '--require', path.join(__dirname,'observe-workers.cjs'), bin,'serve','--mcp','--path',root];
    this.child = spawn(process.execPath,argv,{cwd:root,env:{...process.env,
      CODEGRAPH_NO_DAEMON: direct ? '1' : '0',
      CODEGRAPH_MCP_TOOLS:'explore,node,search,callers,callees,impact,files,status',
      CODEGRAPH_MCP_LOG_ATTACH:'1',
      // Only test-owned daemons: allow prompt graceful exit after clients close.
      CODEGRAPH_DAEMON_IDLE_TIMEOUT_MS:'1000',
      CODEGRAPH_DAEMON_INSTALL_CHECK_MS:'0'},stdio:['pipe','pipe','pipe'],windowsHide:true});
    this.child.on('error',error=>this.fail(error));
    this.child.stdin.on('error',error=>this.fail(error));
    this.child.stderr.on('data',data=>{
      this.stderr += data.toString();
      if(this.stderr.length > 1024*1024) this.fail(new Error('client stderr overflow'));
    });
    this.child.stdout.on('data',data=>{
      this.buffer += data.toString();
      if(this.buffer.length > 10*1024*1024) return this.fail(new Error('client stdout overflow'));
      while(this.buffer.includes('\n')) {
        const index=this.buffer.indexOf('\n');const line=this.buffer.slice(0,index);this.buffer=this.buffer.slice(index+1);
        fs.appendFileSync(path.join(out,`${name}.rpc.jsonl`),line+'\n');
        let response;try{response=JSON.parse(line);}catch{continue;}
        const pending=this.pending.get(response.id);
        if(pending){this.pending.delete(response.id);clearTimeout(pending.timer);response.error ? pending.reject(new Error(JSON.stringify(response.error))) : pending.resolve(response.result);}
      }
    });
    this.child.on('exit',(code,signal)=>this.fail(new Error(`client exited ${code}/${signal}`)));
    clients.push(this);
    sessionFacts.push({name,root,pid:this.child.pid,mode:direct?'direct':'shared-daemon'});
  }
  fail(error){for(const pending of this.pending.values()){clearTimeout(pending.timer);pending.reject(error);}this.pending.clear();}
  request(method,params={}) {
    const id=callId++;
    return new Promise((resolve,reject)=>{
      const timer=setTimeout(()=>{this.pending.delete(id);reject(new Error(`client deadline ${method}`));},75000);
      this.pending.set(id,{resolve,reject,timer});
      this.child.stdin.write(JSON.stringify({jsonrpc:'2.0',id,method,params})+'\n');
    });
  }
  async initialize(root) {
    const started=performance.now();
    const result=await this.request('initialize',{protocolVersion:'2024-11-05',capabilities:{},clientInfo:{name:this.name,version:'1'},rootUri:pathToFileURL(root).href});
    this.child.stdin.write(JSON.stringify({jsonrpc:'2.0',method:'notifications/initialized',params:{}})+'\n');
    timings.push({operation:'startup',session:this.name,milliseconds:performance.now()-started});return result;
  }
  async tool(name,args={}) {
    const started=performance.now();const result=await this.request('tools/call',{name,arguments:args});
    timings.push({operation:'query',tool:name,session:this.name,milliseconds:performance.now()-started});return result;
  }
  async close() {
    if(this.child.exitCode===null&&this.child.signalCode===null) {
      const exit=new Promise(resolve=>this.child.once('exit',resolve));
      this.child.stdin.end();
      await Promise.race([exit,delay(15000).then(()=>{throw new Error(`client failed graceful exit ${this.name}`);})]);
    }
    fs.writeFileSync(path.join(out,`${this.name}.stderr.log`),this.stderr);
  }
}
const textOf = result => JSON.stringify(result);
const searchHas = (result,name) => !result.isError && textOf(result).includes(name) && textOf(result).includes('src/leaf.ts') && !textOf(result).includes('No results found');
async function waitFor(checker,seconds,label) {
  const until=performance.now()+seconds*1000;
  while(performance.now()<until){if(await checker())return;await delay(100);}
  throw new Error(`deadline: ${label}`);
}

async function main(){
  phaseStart(scenarios[0]);
  const roots=['alpha','beta'].map(name=>path.join(process.env.VALIDATION_PROJECT_ROOT||path.join(out,'projects'),name));
  const inputs=[];
  for(const [index,root] of roots.entries()) {
    fs.mkdirSync(path.join(root,'src'),{recursive:true});
    fs.writeFileSync(path.join(root,'.gitignore'),'.codegraph/\ngenerated/\n');
    fs.writeFileSync(path.join(root,'codegraph.json'),'{}\n');
    fs.writeFileSync(path.join(root,'src/app.ts'),"import { service } from './service';\nexport function app() { return service(); }\n");
    fs.writeFileSync(path.join(root,'src/service.ts'),"import { leaf } from './leaf';\nexport function service() { return leaf(); }\n");
    fs.writeFileSync(path.join(root,'src/leaf.ts'),`export function leaf() { return ${index+1}; }\n`);
    const git=spawnSync('git',['init','--quiet',root],{encoding:'utf8',windowsHide:true});
    check(phase,'disposable Git fixture initialized',git.status===0,{project:index});
    const scanStart=performance.now();const discovered=scanDirectory(root).map(file=>(path.isAbsolute(file)?path.relative(root,file):file).replaceAll('\\','/')).sort();
    timings.push({operation:'discovery',project:index,milliseconds:performance.now()-scanStart});
    check(phase,'independent eligible source inventory',JSON.stringify(discovered)===JSON.stringify(['src/app.ts','src/leaf.ts','src/service.ts']),{discovered});
    const initStart=performance.now();const cg=await CodeGraph.init(root);
    try{const result=await cg.indexAll();check(phase,'initial index fully completed',result.success&&result.filesIndexed===3,{result});}
    finally{cg.close();}
    timings.push({operation:'initial-index',project:index,milliseconds:performance.now()-initStart});
    const snapshot=semantic(root);write(`${index}.initial-semantic.json`,snapshot);
    check(phase,'expected app service leaf symbols', ['app','service','leaf'].every(name=>snapshot.nodes.some(node=>node.name===name&&node.kind==='function')));
    check(phase,'independent expected call edges', [['src/app.ts:function:','src/service.ts:function:'],['src/service.ts:function:','src/leaf.ts:function:']].every(([source,target])=>snapshot.edges.some(edge=>edge.kind==='calls'&&edge.source.startsWith(source)&&edge.target.startsWith(target))));
    const facts={name:path.basename(root),root:fs.realpathSync(root),index:path.join(fs.realpathSync(root),'.codegraph/codegraph.db'),initialSemanticSha256:hash(JSON.stringify(snapshot)),activeIgnoreSources:['.gitignore','.git/info/exclude'],configuration:'codegraph.json',gitConfigurationSources:['.git/config','sandbox-global-git-config(absent)','system(disabled)']};
    projectFacts.push(facts);inputs.push({root,initial:fixtureInputs(root)});
  }
  write('fixture-inputs-initial.json',inputs);phaseEnd();
  phaseStart(scenarios[1]);
  const a1=new Client(roots[0],'alpha-1');await a1.initialize(roots[0]);
  const a2=new Client(roots[0],'alpha-2');await a2.initialize(roots[0]);
  const b1=new Client(roots[1],'beta-1');await b1.initialize(roots[1]);
  const tools=await a1.request('tools/list');
  check(phase,'configured eight tools remain exposed',tools.tools.length===8,{tools:tools.tools.map(tool=>tool.name)});
  const results=await Promise.all([a1.tool('codegraph_explore',{query:'app service leaf'}),a2.tool('codegraph_search',{query:'leaf'}),b1.tool('codegraph_explore',{query:'app service leaf'})]);
  check(phase,'three real sessions answer expected graph queries',results.every(result=>!result.isError&&textOf(result).includes('leaf')));
  check(phase,'flow retains every endpoint', ['app','service','leaf'].every(name=>textOf(results[0]).includes(name)));
  const firstPid=daemonPid(roots[0]);
  check(phase,'two sessions share one project daemon',!!firstPid&&firstPid===daemonPid(roots[0])&&firstPid!==a1.child.pid&&firstPid!==a2.child.pid,{daemonPid:firstPid});
  check(phase,'simultaneous projects have distinct owners',!!daemonPid(roots[1])&&daemonPid(roots[1])!==firstPid,{betaDaemonPid:daemonPid(roots[1])});
  phaseEnd();phaseStart(scenarios[2]);
  await a1.close();await a2.close();
  await waitFor(()=>!alive(firstPid),20,'alpha daemon closes before reopen');
  fs.writeFileSync(path.join(roots[0],'src/leaf.ts'),'export function leaf() { return 5; }\nexport function afterClose() { return leaf(); }\n');
  const reopen=new Client(roots[0],'alpha-reopen');const catchupStart=performance.now();await reopen.initialize(roots[0]);
  await waitFor(async()=>searchHas(await reopen.tool('codegraph_search',{query:'afterClose'}),'afterClose'),45,'reopen catchup');
  timings.push({operation:'reopen-catchup',project:0,milliseconds:performance.now()-catchupStart});
  check(phase,'catchup source appears through real MCP',searchHas(await reopen.tool('codegraph_search',{query:'afterClose'}),'afterClose'));
  check(phase,'other project remains available during catchup',searchHas(await b1.tool('codegraph_search',{query:'leaf'}),'leaf'));
  phaseEnd();phaseStart(scenarios[3]);
  write('idle-start.json',{atEpochMs:Date.now(),resourceSampleIndex:sampleIndex(),daemonPids:roots.map(daemonPid)});
  await delay(45000);
  write('idle-end.json',{atEpochMs:Date.now(),resourceSampleIndex:sampleIndex(),daemonPids:roots.map(daemonPid)});
  check(phase,'live quiet sessions remain available after declared idle observation',searchHas(await reopen.tool('codegraph_search',{query:'afterClose'}),'afterClose')&&searchHas(await b1.tool('codegraph_search',{query:'leaf'}),'leaf'));
  await reopen.close();await b1.close();
  await waitFor(()=>roots.every(root=>{const pid=daemonPid(root);return !pid||!alive(pid);}),20,'owned shared daemons exit');
  phaseEnd();phaseStart(scenarios[4]);
  const direct=new Client(roots[0],'alpha-direct',true);await direct.initialize(roots[0]);
  const directResult=await direct.tool('codegraph_explore',{query:'app service leaf'});
  check(phase,'direct session preserves expected flow',!directResult.isError&&['app','service','leaf'].every(name=>textOf(directResult).includes(name)));
  await direct.close();
  for(const [index,root] of roots.entries()) {
    const snapshot=semantic(root);write(`${index}.final-semantic.json`,snapshot);
    projectFacts[index].finalSemanticSha256=hash(JSON.stringify(snapshot));
    inputs[index].final=fixtureInputs(root);
  }
  check(phase,'unchanged simultaneous project preserves semantic data',projectFacts[1].initialSemanticSha256===projectFacts[1].finalSemanticSha256);
  const second=CodeGraph.openSync(roots[0]);let noOp;try{noOp=await second.sync();}finally{second.close();}
  check(phase,'second reconciliation is a no-op',noOp.filesAdded===0&&noOp.filesModified===0&&noOp.filesRemoved===0,{noOp});
  write('fixture-inputs.json',inputs);write('timings.json',timings);write('inventory.json',{projects:projectFacts,sessions:sessionFacts});
  write('effective-baseline-settings.json',{queryPoolSizeOverride:null,busyDeadlineOverride:null,sharedPoolDefault:'clamp(cores-1,1,16)',directPoolDefaultMax:2,busyDeadlineMs:45000,protocolControls:{daemonIdleTimeoutMs:1000,daemonInstallCheckMs:0,mcpTools:['explore','node','search','callers','callees','impact','files','status'],gitSystemConfigDisabled:true,homeRegistry:'runner-owned sandbox',wasmRuntimeFlags:flags,sessionModes:sessionFacts.map(({name,mode})=>({name,mode,noDaemon:mode==='direct'?'1':'0'}))},nativeWatches:process.platform==='linux'?'sampled via /proc':'not applicable on Windows',runtime:process.versions,fixtureVersion:'two-project-flow-v1'});
  phaseEnd();emit('final',{passed:true,scenarios});
}
main().catch(error=>{console.error(error.stack);process.exitCode=1;}).finally(async()=>{
  for(const client of clients){try{await client.close();}catch(error){console.error(error.message);process.exitCode=1;}}
});

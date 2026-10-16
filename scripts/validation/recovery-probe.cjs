// Observable exact-B recovery protocol. All roots and mutable state are owned.
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { spawn, spawnSync } = require('node:child_process');
const { pathToFileURL } = require('node:url');
const { DatabaseSync } = require('node:sqlite');
const out = process.env.VALIDATION_OUT;
const recipe = JSON.parse(fs.readFileSync(process.env.RECOVERY_RECIPE, 'utf8'));
const components = path.join(path.dirname(process.env.RECOVERY_RECIPE), 'components');
const { default: CodeGraph } = require(path.join(components, 'engine/lib/dist/index.js'));
const git = path.join(components, 'git/cmd/git.exe');
const recovery = path.join(__dirname, 'recovery.py');
const roots = ['alpha', 'beta'].map(name => path.join(out, 'projects', name));
const clients = [];
const sessions = [];
const projects = [];
const cleanup = [];
let id = 1;
let phase;
const emit = (type, fields = {}) => fs.appendFileSync(process.env.VALIDATION_EVENTS, JSON.stringify({ type, atEpochSeconds: Date.now()/1000, ...fields })+'\n');
const check = (name, passed, evidence = {}) => { emit('assert', { scenario: phase, name, passed: !!passed, ...evidence }); if (!passed) throw new Error(name); };
const start = name => { phase = name; emit('start', { scenario: name }); };
const complete = () => emit('complete', { scenario: phase });
const write = (name, value) => fs.writeFileSync(path.join(out, name), JSON.stringify(value, null, 2)+'\n');
const hash = data => crypto.createHash('sha256').update(data).digest('hex');
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
const dbPath = root => path.join(root, '.codegraph/codegraph.db');
const sourceInputs = root => Object.fromEntries(fs.readdirSync(path.join(root, 'src')).sort().map(name => {
  const bytes = fs.readFileSync(path.join(root, 'src', name));
  return ['src/'+name, { sha256: hash(bytes), utf8: bytes.toString('utf8') }];
}));
function semantic(file) {
  const db = new DatabaseSync(file, { readOnly: true });
  try {
    db.exec('BEGIN');
    const files = db.prepare('SELECT path,content_hash,language,size,generated FROM files ORDER BY path').all();
    const rows = db.prepare('SELECT id,kind,name,qualified_name,file_path,start_line,end_line FROM nodes ORDER BY file_path,start_line,kind,name').all();
    const keys = new Map(rows.map(row => [row.id, [row.file_path,row.kind,row.qualified_name,row.name,row.start_line,row.end_line].join(':')]));
    const nodes = rows.map(({id,...row}) => row);
    const edges = db.prepare('SELECT source,target,kind,metadata,line,col,provenance FROM edges').all().map(({source,target,...edge}) => ({source:keys.get(source),target:keys.get(target),...edge})).sort((a,b)=>JSON.stringify(a).localeCompare(JSON.stringify(b)));
    const versions = db.prepare('SELECT * FROM schema_versions ORDER BY version').all();
    const metadata = db.prepare('SELECT key,value FROM project_metadata ORDER BY key').all();
    db.exec('COMMIT');
    return { files, nodes, edges, versions, metadata };
  } finally { db.close(); }
}
const graphOnly = value => ({ files:value.files, nodes:value.nodes, edges:value.edges });
function callRecovery(operation, ...args) {
  const result = spawnSync(process.env.RECOVERY_PYTHON, [recovery, operation, ...args, process.env.RECOVERY_OWNER], { encoding:'utf8', windowsHide:true });
  fs.appendFileSync(path.join(out, 'snapshot-operations.jsonl'), JSON.stringify({ operation,args,status:result.status,stdout:result.stdout,stderr:result.stderr })+'\n');
  check('snapshot operation completes through the recovery boundary', result.status === 0, { operation, status:result.status, stderr:result.stderr });
}
class Client {
  constructor(root, name) {
    this.root = root; this.name = name; this.pending = new Map(); this.buffer=''; this.stderr='';
    this.child = spawn(recipe.launch.command, recipe.launch.args, { cwd:root, env:{...process.env,...recipe.launch.env,CODEGRAPH_NO_DAEMON:'1'}, stdio:['pipe','pipe','pipe'],windowsHide:true });
    this.child.on('error', error=>this.fail(error));
    this.child.stdin.on('error', error=>this.fail(error));
    this.child.stderr.on('data', data=>{this.stderr+=data.toString();if(this.stderr.length>1024*1024)this.fail(new Error('MCP stderr overflow'));});
    this.child.stdout.on('data', data=>{
      this.buffer+=data.toString();if(this.buffer.length>10*1024*1024)return this.fail(new Error('MCP stdout overflow'));
      while(this.buffer.includes('\n')) {
        const end=this.buffer.indexOf('\n');const line=this.buffer.slice(0,end);this.buffer=this.buffer.slice(end+1);
        fs.appendFileSync(path.join(out,name+'.rpc.jsonl'),line+'\n');
        let value;try{value=JSON.parse(line);}catch{continue;}
        const pending=this.pending.get(value.id);if(pending){this.pending.delete(value.id);clearTimeout(pending.timer);value.error?pending.reject(new Error(JSON.stringify(value.error))):pending.resolve(value.result);}
      }
    });
    this.child.on('exit',(code,signal)=>this.fail(new Error(`MCP exited ${code}/${signal}`)));
    clients.push(this);sessions.push({name,root,pid:this.child.pid,mode:'exact-B-relocated-proxy-direct'});
  }
  fail(error) {for(const pending of this.pending.values()){clearTimeout(pending.timer);pending.reject(error);}this.pending.clear();}
  request(method,params={}) {
    const requestId=id++;
    return new Promise((resolve,reject)=>{
      const timer=setTimeout(()=>{this.pending.delete(requestId);reject(new Error('host probe deadline '+method));},75000);
      this.pending.set(requestId,{resolve,reject,timer});this.child.stdin.write(JSON.stringify({jsonrpc:'2.0',id:requestId,method,params})+'\n');
    });
  }
  async initialize() {
    await this.request('initialize',{protocolVersion:'2024-11-05',capabilities:{},clientInfo:{name:this.name,version:'1'},rootUri:pathToFileURL(this.root).href});
    this.child.stdin.write(JSON.stringify({jsonrpc:'2.0',method:'notifications/initialized'})+'\n');
  }
  tool(name,args={}) {return this.request('tools/call',{name,arguments:{...args,projectPath:this.root}});}
  async close() {
    if(this.closed)return;
    if(this.child.exitCode===null&&this.child.signalCode===null){
      const exited=new Promise(resolve=>this.child.once('exit',resolve));this.child.stdin.end();
      let timer;try{await Promise.race([exited,new Promise((_,reject)=>{timer=setTimeout(()=>reject(new Error('MCP graceful-close deadline')),15000);})]);}finally{clearTimeout(timer);}
    }
    if(this.child.exitCode!==0||this.child.signalCode!==null)throw new Error(`MCP cleanup exit ${this.child.exitCode}/${this.child.signalCode}`);
    fs.writeFileSync(path.join(out,this.name+'.stderr.log'),this.stderr);this.closed=true;
    cleanup.push({name:this.name,pid:this.child.pid,exitCode:this.child.exitCode,complete:true});
  }
}
const has = (result,name) => !result.isError && JSON.stringify(result).includes(name) && !JSON.stringify(result).includes('No results found');
async function main() {
  start('B-compatible-index');
  for(const [index,root] of roots.entries()){
    fs.mkdirSync(path.join(root,'src'),{recursive:true});
    fs.writeFileSync(path.join(root,'.gitignore'),'.codegraph/\n');fs.writeFileSync(path.join(root,'codegraph.json'),'{}\n');
    const init=spawnSync(git,['init','--quiet',root],{encoding:'utf8',windowsHide:true});check('copied B Git initializes an owned canonical root',init.status===0,{index,git});
    if(index===0){
      fs.writeFileSync(path.join(root,'src/app.ts'),"import { leaf } from './leaf';\nexport function app() { return leaf(); }\n");
      fs.writeFileSync(path.join(root,'src/leaf.ts'),'export function leaf() { return 1; }\n');
      fs.writeFileSync(path.join(root,'src/stable.ts'),'export function untouched() { return 37; }\n');
      fs.writeFileSync(path.join(root,'src/removed.ts'),'export function removedLegacy() { return 19; }\n');
    }else fs.writeFileSync(path.join(root,'src/beta.ts'),'export function betaUnique() { return 83; }\n');
    const cg=await CodeGraph.init(root);try{const indexed=await cg.indexAll();check('exact B creates its own compatible pre-change disposable index',indexed.success,{index,indexed});}finally{cg.close();}
    const initial=semantic(dbPath(root));write(index+'.initial-semantic.json',initial);write(index+'.initial-inputs.json',sourceInputs(root));
    projects.push({name:path.basename(root),root:fs.realpathSync(root),index:dbPath(root),initialGraphSha256:hash(JSON.stringify(graphOnly(initial)))});
  }
  const initialAlpha=semantic(dbPath(roots[0]));
  check('independent pre-change graph facts exist', ['app','leaf','untouched','removedLegacy'].every(name=>initialAlpha.nodes.some(node=>node.kind==='function'&&node.name===name)) && initialAlpha.edges.some(edge=>edge.kind==='calls'&&edge.source.includes(':app:')&&edge.target.includes(':leaf:')));
  complete();start('B-ordinary-MCP');
  const alpha=new Client(roots[0],'alpha-initial');await alpha.initialize();
  const beta=new Client(roots[1],'beta-independent');await beta.initialize();
  const list=await alpha.request('tools/list');
  // Exact B predates the candidate: its tiny-project surface is deliberately
  // search/node/explore even with an eight-tool saved allowlist. Preserve B.
  check('B retains its previous tiny-project MCP surface and saved eight-tool allowlist',
    JSON.stringify(list.tools.map(tool=>tool.name).sort())===JSON.stringify(['codegraph_explore','codegraph_node','codegraph_search'])&&
    process.env.CODEGRAPH_MCP_TOOLS==='explore,node,search,callers,callees,impact,files,status',
    {tools:list.tools.map(tool=>tool.name),savedAllowlist:process.env.CODEGRAPH_MCP_TOOLS});
  check('restored B answers ordinary explore and search through its exact launcher',has(await alpha.tool('codegraph_explore',{query:'app leaf'}),'leaf')&&has(await alpha.tool('codegraph_search',{query:'removedLegacy'}),'removedLegacy'));
  check('simultaneous distinct disposable project remains usable',has(await beta.tool('codegraph_search',{query:'betaUnique'}),'betaUnique'));
  complete();start('B-online-snapshot');
  const writer=new DatabaseSync(dbPath(roots[0]));
  const reader=new DatabaseSync(dbPath(roots[0]),{readOnly:true});
  try{
    writer.exec('PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0');
    reader.exec('BEGIN');reader.prepare('SELECT COUNT(*) FROM project_metadata').get();
    writer.prepare("INSERT INTO project_metadata(key,value,updated_at) VALUES ('recoveryProbeWal','committed-before-snapshot',?)").run(Date.now());
    check('graph database has a nonempty committed WAL while disposable owners remain open',fs.statSync(dbPath(roots[0])+'-wal').size>0);
    const mainOnly=path.join(out,'deliberately-incomplete-main-only.db');fs.copyFileSync(dbPath(roots[0]),mainOnly);
    const unsafe=new DatabaseSync(mainOnly,{readOnly:true});try{check('a main-file-only copy demonstrably omits the committed recovery fact',!unsafe.prepare("SELECT value FROM project_metadata WHERE key='recoveryProbeWal'").get());}finally{unsafe.close();}
    callRecovery('snapshot',dbPath(roots[0]),path.join(out,'protected-index'));
  }finally{reader.close();writer.close();}
  const protectedGraph=semantic(path.join(out,'protected-index/codegraph.db'));
  check('consistent snapshot includes the committed WAL fact and complete graph',protectedGraph.metadata.some(row=>row.key==='recoveryProbeWal'&&row.value==='committed-before-snapshot')&&JSON.stringify(graphOnly(protectedGraph))===JSON.stringify(graphOnly(initialAlpha)));
  write('protected-semantic.json',protectedGraph);complete();start('B-restore-catchup');
  // Close the target's only session before legitimate source/index changes.
  await alpha.close();
  fs.writeFileSync(path.join(roots[0],'src/app.ts'),"import { leaf } from './leaf';\nimport { afterBackup } from './added';\nexport function app() { return leaf() + afterBackup(); }\n");
  fs.writeFileSync(path.join(roots[0],'src/leaf.ts'),'export function leaf() { return 5; }\nexport function leafAfterBackup() { return leaf(); }\n');
  fs.writeFileSync(path.join(roots[0],'src/added.ts'),'export function afterBackup() { return 101; }\n');
  fs.unlinkSync(path.join(roots[0],'src/removed.ts'));
  const legitimate=await CodeGraph.open(roots[0]);try{const result=await legitimate.sync();write('legitimate-index-sync.json',result);}finally{legitimate.close();}
  const laterGraph=semantic(dbPath(roots[0]));write('alpha.legitimate-later-semantic.json',laterGraph);
  check('later legitimate source was already indexed before protected-state restore',laterGraph.nodes.some(node=>node.name==='afterBackup')&&!laterGraph.nodes.some(node=>node.name==='removedLegacy'));
  fs.writeFileSync(path.join(roots[1],'src/beta.ts'),'export function betaUnique() { return 83; }\nexport function betaNewer() { return 89; }\n');
  // Native convergence in the independent live owner; no manual sync hides it.
  const until=Date.now()+45000;let betaNewer=false;
  while(Date.now()<until){if(has(await beta.tool('codegraph_search',{query:'betaNewer'}),'betaNewer')){betaNewer=true;break;}await delay(100);}
  check('unrelated newer source/index work is visible before recovery',betaNewer);
  const betaBefore=semantic(dbPath(roots[1]));write('beta.newer-semantic.json',betaBefore);
  const sourceBefore=sourceInputs(roots[0]);write('alpha.later-inputs.json',sourceBefore);
  const retired=path.join(out,'legitimate-later-index-closed');
  fs.renameSync(path.join(roots[0],'.codegraph'),retired); // No target owner remains.
  fs.mkdirSync(path.join(roots[0],'.codegraph'));
  callRecovery('restore-snapshot',path.join(out,'protected-index'),dbPath(roots[0]));
  const beforeCatchup=semantic(dbPath(roots[0]));
  check('exact protected compatible database identity is restored before B opens it',JSON.stringify(beforeCatchup)===JSON.stringify(protectedGraph));
  check('restore leaves later source untouched and retains the later closed index',JSON.stringify(sourceInputs(roots[0]))===JSON.stringify(sourceBefore)&&fs.existsSync(path.join(retired,'codegraph.db')));
  const caught=CodeGraph.openSync(roots[0]);let catchup,noOp;try{catchup=await caught.sync();noOp=await caught.sync();}finally{caught.close();}
  write('explicit-catchup.json',{catchup,noOp});
  check('explicit B catch-up preserves additions modifications and deletions',catchup.filesAdded===1&&catchup.filesModified===2&&catchup.filesRemoved===1);
  check('second B reconciliation is a no-op',noOp.filesAdded===0&&noOp.filesModified===0&&noOp.filesRemoved===0);
  const finalAlpha=semantic(dbPath(roots[0]));write('0.final-semantic.json',finalAlpha);
  check('caught-up graph equals independently retained legitimate later graph',JSON.stringify(graphOnly(finalAlpha))===JSON.stringify(graphOnly(laterGraph)));
  check('untouched content survives restoration and catch-up',sourceBefore['src/stable.ts'].sha256===sourceInputs(roots[0])['src/stable.ts'].sha256&&finalAlpha.nodes.some(node=>node.name==='untouched'));
  const recovered=new Client(roots[0],'alpha-restored');await recovered.initialize();
  check('recovered B serves later work through ordinary host-facing MCP',has(await recovered.tool('codegraph_search',{query:'afterBackup'}),'afterBackup')&&has(await recovered.tool('codegraph_explore',{query:'app afterBackup leaf'}),'afterBackup')&&!has(await recovered.tool('codegraph_search',{query:'removedLegacy'}),'removedLegacy'));
  check('unrelated newer project stays available throughout target recovery',has(await beta.tool('codegraph_search',{query:'betaNewer'}),'betaNewer')&&JSON.stringify(graphOnly(semantic(dbPath(roots[1]))))===JSON.stringify(graphOnly(betaBefore)));
  projects[0].finalGraphSha256=hash(JSON.stringify(graphOnly(finalAlpha)));projects[1].finalGraphSha256=hash(JSON.stringify(graphOnly(betaBefore)));
  write('1.final-semantic.json',betaBefore);write('0.final-inputs.json',sourceInputs(roots[0]));write('1.final-inputs.json',sourceInputs(roots[1]));
  complete();start('B-owned-cleanup');
  for(const client of clients)await client.close();
  check('every owned exact-B MCP launcher exited zero before releasing handles',clients.every(client=>client.closed)&&cleanup.length===clients.length,{cleanup});
  // Fixtures/snapshots/closed later index are retained evidence, never deleted.
  check('compatible snapshot and later index evidence retained after cleanup',fs.existsSync(path.join(out,'protected-index/snapshot.json'))&&fs.existsSync(path.join(retired,'codegraph.db')));
  complete();
}
let primary;
main().catch(error=>{primary=error;console.error(error.stack);process.exitCode=1;}).finally(async()=>{
  for(const client of clients){try{await client.close();}catch(error){cleanup.push({name:client.name,complete:false,error:String(error)});console.error(error.stack);process.exitCode=1;}}
  write('inventory.json',{projects,sessions});write('owned-cleanup.json',{cleanup,primary:primary?String(primary):null,complete:clients.every(client=>client.closed)});
});

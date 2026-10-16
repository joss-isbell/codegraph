// Owned MCP transport used by the exact-B recovery probe.
const fs = require('node:fs');
const path = require('node:path');
const { spawn } = require('node:child_process');
const { pathToFileURL } = require('node:url');
const { StringDecoder } = require('node:string_decoder');
let id = 1;

class Client {
  constructor(root, name, options) {
    this.root = root; this.name = name; this.pending = new Map(); this.buffer='';
    this.out = options.out; this.cleanup = options.cleanup;
    this.stderrLimit = options.stderrLimit ?? 1024*1024;
    this.stdoutLimit = options.stdoutLimit ?? 10*1024*1024;
    this.requestDeadline = options.requestDeadline ?? 75000;
    this.closeDeadline = options.closeDeadline ?? 15000;
    this.decoder = new StringDecoder('utf8');
    this.logs = { stdout:{seenBytes:0,retainedBytes:0,limitBytes:this.stdoutLimit,file:name+'.rpc.jsonl'},
      stderr:{seenBytes:0,retainedBytes:0,limitBytes:this.stderrLimit,file:name+'.stderr.log'} };
    for(const log of Object.values(this.logs))fs.writeFileSync(path.join(this.out,log.file),'',{flag:'wx'});
    this.child = spawn(options.launch.command, options.launch.args, { cwd:root, env:{...process.env,...options.launch.env,CODEGRAPH_NO_DAEMON:'1'}, stdio:['pipe','pipe','pipe'],windowsHide:true });
    this.child.on('error', error=>this.fail(error));
    this.child.stdin.on('error', error=>this.fail(error));
    this.child.stderr.on('data', data=>this.capture('stderr',data));
    this.child.stdout.on('data', data=>{
      if(!this.capture('stdout',data))return;
      this.buffer+=this.decoder.write(data);
      while(this.buffer.includes('\n')) {
        const end=this.buffer.indexOf('\n');const line=this.buffer.slice(0,end);this.buffer=this.buffer.slice(end+1);
        let value;try{value=JSON.parse(line);}catch{continue;}
        const pending=this.pending.get(value.id);if(pending){this.pending.delete(value.id);clearTimeout(pending.timer);value.error?pending.reject(new Error(JSON.stringify(value.error))):pending.resolve(value.result);}
      }
    });
    this.child.on('exit',(code,signal)=>{if(!this.closing||code!==0||signal!==null)this.fail(new Error(`MCP exited ${code}/${signal}`));});
    this.child.on('close',()=>{this.streamsClosed=true;});
    options.sessions?.push({name,root,pid:this.child.pid,mode:'exact-B-relocated-proxy-direct'});
  }
  capture(stream,data) {
    const log=this.logs[stream];log.seenBytes+=data.length;
    const retained=data.subarray(0,Math.max(0,log.limitBytes-log.retainedBytes));
    if(retained.length){fs.appendFileSync(path.join(this.out,log.file),retained);log.retainedBytes+=retained.length;}
    if(log.seenBytes>log.limitBytes){log.overflow=true;this.fail(new Error('MCP '+stream+' overflow'));this.persist();return false;}
    return true;
  }
  persist() {
    fs.writeFileSync(path.join(this.out,this.name+'.output.json'),JSON.stringify({logs:this.logs,
      failure:this.failure?String(this.failure):null,closeFailure:this.closeFailure?String(this.closeFailure):null,
      exitCode:this.child.exitCode,signal:this.child.signalCode,
      streamsClosed:!!this.streamsClosed},null,2)+'\n');
  }
  fail(error) {
    this.failure??=error;
    for(const pending of this.pending.values()){clearTimeout(pending.timer);pending.reject(this.failure);}this.pending.clear();
  }
  request(method,params={}) {
    if(this.failure)return Promise.reject(this.failure);
    const requestId=id++;
    return new Promise((resolve,reject)=>{
      const timer=setTimeout(()=>this.fail(new Error('host probe deadline '+method)),this.requestDeadline);
      this.pending.set(requestId,{resolve,reject,timer});this.child.stdin.write(JSON.stringify({jsonrpc:'2.0',id:requestId,method,params})+'\n');
    });
  }
  async initialize() {
    await this.request('initialize',{protocolVersion:'2024-11-05',capabilities:{},clientInfo:{name:this.name,version:'1'},rootUri:pathToFileURL(this.root).href});
    this.child.stdin.write(JSON.stringify({jsonrpc:'2.0',method:'notifications/initialized'})+'\n');
  }
  tool(name,args={}) {return this.request('tools/call',{name,arguments:{...args,projectPath:this.root}});}
  async close() {
    let error;
    try {
      if(this.closed)return;
      this.closing=true;
      if(!this.streamsClosed){
        const exited=new Promise(resolve=>this.child.once('close',resolve));this.child.stdin.end();
        let timer;try{await Promise.race([exited,new Promise((_,reject)=>{timer=setTimeout(()=>reject(new Error('MCP graceful-close deadline')),this.closeDeadline);})]);}finally{clearTimeout(timer);}
      }
      if(this.child.exitCode!==0||this.child.signalCode!==null)throw new Error(`MCP cleanup exit ${this.child.exitCode}/${this.child.signalCode}`);
      if(this.failure)throw this.failure;
      this.closed=true;
    } catch(failure) {
      this.closeFailure??=failure;
      error=failure;throw failure;
    } finally {
      this.persist();
      const status={name:this.name,pid:this.child.pid,exitCode:this.child.exitCode,signal:this.child.signalCode,
        complete:!!this.closed,streamsClosed:!!this.streamsClosed,error:error?String(error):null};
      if(this.cleanupStatus)Object.assign(this.cleanupStatus,status);
      else{this.cleanupStatus=status;this.cleanup.push(status);}
    }
  }
}
module.exports = { Client };

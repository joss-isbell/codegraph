// Read-only observation of Node's public Worker interface; no product policy changes.
const fs=require('node:fs');const path=require('node:path');const threads=require('node:worker_threads');
if(threads.isMainThread&&process.env.VALIDATION_OUT){
  const RealWorker=threads.Worker;
  const record=value=>fs.appendFileSync(path.join(process.env.VALIDATION_OUT,'workers.jsonl'),JSON.stringify({atEpochMs:Date.now(),pid:process.pid,...value})+'\n');
  threads.Worker=class ObservedWorker extends RealWorker{
    constructor(filename,options){super(filename,options);const threadId=this.threadId;const file=String(filename);record({type:'spawn',threadId,file});
      this.once('message',message=>record({type:'first-message',threadId,file,handshake:message?.type??null}));
      this.once('exit',code=>record({type:'exit',threadId,file,code}));}
  };
}

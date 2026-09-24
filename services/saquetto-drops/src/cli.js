import { readFileSync } from 'node:fs';
import { loadConfig, matchRules } from './config.js';

const [command,arg] = process.argv.slice(2);
try {
  if (command === 'validate') {
    const {config}=loadConfig(arg || process.env.DROPS_CONFIG || '/etc/saquetto-drops/config.json');
    console.log(JSON.stringify({valid:true,sources:config.sources,rules:config.rules.length,paused:config.operation.paused,dryRun:config.operation.dryRun}));
  } else if (command === 'simulate') {
    const {config}=loadConfig(process.env.DROPS_CONFIG || '/etc/saquetto-drops/config.json');
    const post=JSON.parse(readFileSync(arg,'utf8'));
    console.log(JSON.stringify({destinations:matchRules(config,post),sent:false}));
  } else {
    const route={status:['GET','/v1/status'],pending:['GET','/v1/pending'],pause:['POST','/v1/pause'],
      reload:['POST','/v1/config/reload'],activate:['POST','/v1/activate']}[command];
    if(!route)throw new Error('usage: validate [config] | simulate post.json | status | pending | pause | reload | activate');
    const token=process.env.DROPS_TOKEN;if(!token)throw new Error('DROPS_TOKEN_required');
    const response=await fetch(`http://127.0.0.1:${Number(process.env.DROPS_PORT || 8788)}${route[1]}`,{
      method:route[0],headers:{Authorization:`Bearer ${token}`},redirect:'error',signal:AbortSignal.timeout(10000)});
    console.log(JSON.stringify(await response.json()));if(!response.ok)process.exitCode=1;
  }
} catch {console.error(JSON.stringify({code:'command_failed'}));process.exitCode=1;}

import { readFileSync, chmodSync } from 'node:fs';
import QRCode from 'qrcode';

process.umask(0o077);
const [input,output]=process.argv.slice(2);
if(!input||!output)throw new Error('private_input_and_output_paths_required');
const qr=readFileSync(input,'utf8');
if(!qr.length||qr.length>4096)throw new Error('invalid_pairing_qr');
await QRCode.toFile(output,qr,{width:560,margin:4});
chmodSync(output,0o600);
console.log('qr_rendered');

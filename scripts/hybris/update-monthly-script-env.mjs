import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { HybrisClient } from '../../dist/hybris-client.js';

process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

const envKey = process.argv[2];
const localPath = process.argv[3];
if (!envKey || !localPath) {
  console.error('Usage: node update-monthly-script-env.mjs <mcpEnvKey> <localGroovyPath>');
  process.exit(1);
}

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, '../..');
const absLocal = path.isAbsolute(localPath) ? localPath : path.join(root, localPath);
const content = fs.readFileSync(absLocal, 'utf8').replace(/\r\n/g, '\n');
const b64 = Buffer.from(content, 'utf8').toString('base64');

const mcp = JSON.parse(
  fs.readFileSync('/Users/pdwiv/Project/Code/AI/hybris-mcp/.cursor/mcp.json', 'utf8')
);
const env = mcp.mcpServers[envKey]?.env;
if (!env) {
  console.error('Unknown MCP env key:', envKey);
  process.exit(1);
}

const c = new HybrisClient({
  baseUrl: env.HYBRIS_BASE_URL.replace(/\/$/, ''),
  username: env.HYBRIS_USERNAME,
  password: env.HYBRIS_PASSWORD,
  hacPath: env.HYBRIS_HAC_PATH ?? '',
  occPath: env.HYBRIS_OCC_PATH || '/monsantocommercewebservices/v2',
});

const CODE = 'cbusOrderStepTimingsMonthlyScript';

async function run(script, commit = false) {
  const res = await c.executeGroovyScript(script, commit);
  if (res.stacktraceText) {
    console.error(res.stacktraceText);
    process.exit(1);
  }
  return res;
}

const meta = await run(`
import de.hybris.platform.servicelayer.search.FlexibleSearchQuery
def fss = spring.getBean('flexibleSearchService')
def q = new FlexibleSearchQuery('SELECT {pk},{active},{content},{modifiedtime} FROM {Script} WHERE {code}=?c')
q.addQueryParameter('c','${CODE}')
q.setResultClassList([de.hybris.platform.core.PK.class, Boolean.class, String.class, Date.class])
def rows = fss.search(q).result
if (!rows) return 'NONE'
def scored = rows.collect { r -> [pk:r[0].toString(), active:r[1], len:(r[2]?.length()?:0), mod:String.valueOf(r[3])] }
  .sort { a,b -> ((b.active?1:0) <=> (a.active?1:0)) ?: (b.len <=> a.len) }
return scored[0].pk + '|' + scored[0].active + '|' + scored[0].len + '|' + scored[0].mod + '|' + rows.size()
`);
console.log('before', meta.result);
if (String(meta.result) === 'NONE') {
  console.error('No Script found for', CODE);
  process.exit(2);
}
const [bestPk, active, oldLen] = String(meta.result).split('|');
console.log('Updating PK', bestPk, 'active=', active, 'oldLen=', oldLen);
console.log('Local chars', content.length, 'b64len', b64.length);

// Accumulate base64 in session across chunks
const chunkSize = 12000;
const init = await run(`
def ss = spring.getBean('sessionService')
ss.getCurrentSession().setAttribute('__scriptUploadB64', '')
return 'ok'
`);
console.log('init', init.result);

for (let offset = 0; offset < b64.length; offset += chunkSize) {
  const chunk = b64.slice(offset, Math.min(offset + chunkSize, b64.length));
  // Escape for Groovy single-quoted string: \' and \\
  const escaped = chunk.replace(/\\/g, '\\\\').replace(/'/g, "\\'");
  const part = await run(`
def ss = spring.getBean('sessionService')
def cur = (ss.getCurrentSession().getAttribute('__scriptUploadB64') ?: '') as String
ss.getCurrentSession().setAttribute('__scriptUploadB64', cur + '${escaped}')
return String.valueOf(((ss.getCurrentSession().getAttribute('__scriptUploadB64') ?: '') as String).length())
`);
  console.log('chunk', offset, Math.min(offset + chunkSize, b64.length), 'acc', part.result);
}

const expectedB64Len = b64.length;
const save = await run(
  `
import de.hybris.platform.core.PK
import java.util.Base64
def ss = spring.getBean('sessionService')
def uploaded = (ss.getCurrentSession().getAttribute('__scriptUploadB64') ?: '') as String
if (uploaded.length() != ${expectedB64Len}) {
  return 'LEN_MISMATCH:' + uploaded.length() + ':' + ${expectedB64Len}
}
def decoded = new String(Base64.getDecoder().decode(uploaded), 'UTF-8')
def s = modelService.get(PK.parse('${bestPk}'))
def beforeLen = (s.content ?: '').length()
s.setContent(decoded)
modelService.save(s)
ss.getCurrentSession().removeAttribute('__scriptUploadB64')
return 'SAVED|' + s.pk + '|before=' + beforeLen + '|after=' + decoded.length()
`,
  true
);
console.log('save', save.result);

const verify = await run(`
import de.hybris.platform.core.PK
import java.util.Base64
def s = modelService.get(PK.parse('${bestPk}'))
def body = s.content ?: ''
def b64 = Base64.getEncoder().encodeToString(body.getBytes('UTF-8'))
return s.pk.toString() + '|' + s.active + '|' + body.length() + '|' + String.valueOf(s.modifiedtime) + '|' + b64.substring(0, Math.min(32, b64.length()))
`);
console.log('after', verify.result);

const localHead = Buffer.from(content, 'utf8').toString('base64').slice(0, 32);
const remoteHead = String(verify.result).split('|').pop();
if (remoteHead !== localHead) {
  console.error('HEAD_MISMATCH local', localHead, 'remote', remoteHead);
  process.exit(3);
}
console.log('Verified content head matches local file.');

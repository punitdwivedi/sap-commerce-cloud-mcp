import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { HybrisClient } from '../../dist/hybris-client.js';

process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

const envKey = process.argv[2]; // e.g. hybris_p1_admin
const outPath = process.argv[3];
if (!envKey || !outPath) {
  console.error('Usage: node fetch-monthly-script-env.mjs <mcpEnvKey> <outPath>');
  process.exit(1);
}

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, '../..');
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

const meta = await c.executeGroovyScript(
  `
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
`,
  false
);
if (meta.stacktraceText) {
  console.error(meta.stacktraceText);
  process.exit(1);
}
console.log('meta', meta.result);
if (String(meta.result) === 'NONE') {
  console.error('No Script found for', CODE);
  process.exit(2);
}
const [bestPk, active, rawLen, mod, total] = String(meta.result).split('|');

const head = await c.executeGroovyScript(
  `
import de.hybris.platform.core.PK
import java.util.Base64
def s = modelService.get(PK.parse('${bestPk}'))
def b64 = Base64.getEncoder().encodeToString((s.content ?: '').getBytes('UTF-8'))
return String.valueOf(b64.length())
`,
  false
);
if (head.stacktraceText) {
  console.error(head.stacktraceText);
  process.exit(1);
}
const exactB64Len = Number(head.result);
console.log('pk', bestPk, 'contentLen', rawLen, 'b64len', exactB64Len);

let b64 = '';
const chunkSize = 15000;
for (let offset = 0; offset < exactB64Len; offset += chunkSize) {
  const end = Math.min(offset + chunkSize, exactB64Len);
  const part = await c.executeGroovyScript(
    `
import de.hybris.platform.core.PK
import java.util.Base64
def s = modelService.get(PK.parse('${bestPk}'))
def b64 = Base64.getEncoder().encodeToString((s.content ?: '').getBytes('UTF-8'))
return b64.substring(${offset}, ${end})
`,
    false
  );
  if (part.stacktraceText) {
    console.error(part.stacktraceText);
    process.exit(1);
  }
  b64 += String(part.result || '');
  console.log('chunk', offset, end);
}

const content = Buffer.from(b64, 'base64').toString('utf8');
const absOut = path.isAbsolute(outPath) ? outPath : path.join(root, outPath);
fs.mkdirSync(path.dirname(absOut), { recursive: true });
fs.writeFileSync(absOut, content);
fs.writeFileSync(
  absOut + '.meta.txt',
  [
    `code=${CODE}`,
    `env=${envKey}`,
    `baseUrl=${env.HYBRIS_BASE_URL}`,
    `pk=${bestPk}`,
    `active=${active}`,
    `contentLen=${content.length}`,
    `modified=${mod}`,
    `duplicateScriptRows=${total}`,
    `fetchedAt=${new Date().toISOString()}`,
    `file=${absOut}`,
  ].join('\n') + '\n'
);
console.log('Wrote', absOut, 'chars', content.length);

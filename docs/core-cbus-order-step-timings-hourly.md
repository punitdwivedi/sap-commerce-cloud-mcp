# core_cbus Order Step Timings — Hourly Report

## Process summary

Every hour, SAP Commerce (P1) runs a Groovy ScriptingJob that:

1. Looks back over a configurable window (default **1 hour**) for **SUCCEEDED** `OrderProcess` rows on store **`core_cbus`**.
2. Classifies each process as **CREATE** (`sap-order-process*`) or **MODIFY** (everything else).
3. Loads `ProcessTaskLog` rows and derives timing columns in **milliseconds**:
   - Cart submit → order conversion (`cartToOrderMs`)
   - Commerce persist → business process start
   - BP → first task / first ERP task
   - Per-step ERP API and wall times (CREATE: Steps 1–5 from `sendOrderToSAP` logs; MODIFY: prepay / OData / LCR / brand+SF)
4. Builds a CSV named `core_cbus-order-step-timings-yyyyMMdd-HHmm.csv` (UTC stamp).
5. **Emails** the CSV via `bayerEmailService` (same path as `coreUsOrderMonitorScript`).
6. **Uploads** the CSV to S3 bucket `channel-us-order-step-timings` (bucket root by default) using Vault-backed AWS credentials (`onedce.aws.path`).

Schedule: CronJob trigger `0 5 * * * ?` (minute **:05** every hour).

---

## Architecture

```mermaid
flowchart LR
  subgraph schedule [Schedule]
    T[Trigger :05 hourly] --> CJ[CronJob]
    CJ --> SJ[ScriptingJob]
    SJ --> SC[Script GROOVY]
  end

  subgraph extract [Extract]
    SC --> FS[FlexibleSearch OrderProcess SUCCEEDED]
    FS --> PTL[ProcessTaskLog per process]
    PTL --> CSV[CSV in memory]
  end

  subgraph deliver [Deliver]
    CSV --> EM[bayerEmailService]
    CSV --> S3[bayerAmazonS3IntService putObject]
    EM --> Mail[Recipients]
    S3 --> Bucket[channel-us-order-step-timings]
  end
```

### Hybris types / codes

| Type | Code | Role |
|------|------|------|
| `Script` | `coreCbusOrderStepTimingsHourlyScript` | Groovy body (maintain in Backoffice / HAC; do **not** wipe via ImpEx content) |
| `ScriptingJob` | `coreCbusOrderStepTimingsHourlyJob` | `scriptURI = model://coreCbusOrderStepTimingsHourlyScript` |
| `CronJob` | `coreCbusOrderStepTimingsHourlyCronJob` | Runs the job as `admin` |
| `Trigger` | on CronJob | `0 5 * * * ?`, active |

### Source files (repo)

| File | Purpose |
|------|---------|
| [`scripts/hybris/core-cbus-order-step-timings-hourly.groovy`](../scripts/hybris/core-cbus-order-step-timings-hourly.groovy) | Full report logic (authoritative source for Script content) |
| [`scripts/hybris/core-cbus-order-step-timings-hourly.impex`](../scripts/hybris/core-cbus-order-step-timings-hourly.impex) | Registers Script / Job / CronJob / Trigger (no Script content) |
| [`scripts/hybris/core-cbus-order-step-timings-hourly.properties`](../scripts/hybris/core-cbus-order-step-timings-hourly.properties) | Commented property overrides for CCV2 / local |
| [`scripts/export-core-cbus-order-step-timings.mjs`](../scripts/export-core-cbus-order-step-timings.mjs) | Ad-hoc Node export (same CSV shape; used for 7-day one-offs) |

---

## Artifact details

### Groovy — `core-cbus-order-step-timings-hourly.groovy`

Paste this file’s body into Hybris `Script` **`coreCbusOrderStepTimingsHourlyScript`** (`GROOVY`). This is the only place the report logic should live; keep git and the env Script content in sync.

**Responsibilities**

| Block | What it does |
|-------|----------------|
| Config helpers | `cfg` / `cfgBool` / `parseEmailList` read `Config.getParameter`; empty → hardcoded default |
| Window | `HOURS_BACK` hours ending at job start; UTC file stamp `core_cbus-order-step-timings-yyyyMMdd-HHmm.csv` |
| CSV build | FlexibleSearch SUCCEEDED `OrderProcess` for `STORE`; load `ProcessTaskLog`; `buildRow` → CREATE/MODIFY timing columns (ms) |
| Email | `bayerEmailService` + `Email` / `EmailAttachment` (base64); From = `onedce.general.mailbox` else `email.from` |
| S3 | `bayerAmazonS3IntService.getOneDCEs3Client(region)` + `putObject` to `channel-us-order-step-timings` (Vault via `onedce.aws.path`) |
| Return | `OK rows=N file=… email=… s3=… window=…` (also logged under `coreCbusOrderStepTimingsHourly`) |

**Hardcoded defaults (when property unset)**

```text
store          = core_cbus
hoursBack      = 1
email.to       = punit.dwivedi.ext@bayer.com
email.from     = donot-reply@channelarrow.com
s3.bucket      = channel-us-order-step-timings
s3.region      = us-east-1
s3.prefix      = (empty → object key = file name at bucket root)
skipEmail/S3   = false
```

**Beans / deps used in script**

- `flexibleSearchService`, `configurationService`, `bayerEmailService`, `bayerAmazonS3IntService`
- Types: `Email`, `EmailAttachment`, AWS `ObjectMetadata` / `Regions`
- Config keys beyond report props: `onedce.aws.path`, `onedce.general.mailbox`, `env` (for `[NON-PROD]` subject prefix)

**Do not** store AWS access keys in the Groovy or properties — Vault only.

---

### Properties — `core-cbus-order-step-timings-hourly.properties`

Reference / copy-paste for CCV2 aspect or `local.properties`. **All lines are commented**; the Groovy already applies the same defaults if keys are absent. Uncomment only what you need to override.

```properties
# Optional CCV2 / local.properties overrides for hourly order step-timings report.
# Values below are hardcoded as Groovy fallbacks if unset.
#
# Already on env (do not redeclare unless overriding):
#   onedce.aws.path   (d1: secret/explorers_onedce/aws-np, p1: .../aws-prod)
#   vault.url

# order.timings.report.store=core_cbus
# order.timings.report.hoursBack=1
# order.timings.report.email.to=punit.dwivedi.ext@bayer.com,other@bayer.com
# order.timings.report.email.from=donot-reply@channelarrow.com
# order.timings.report.email.cc=cc1@bayer.com;cc2@bayer.com

# Dedicated S3 bucket — CSV uploaded at bucket root by default
# order.timings.report.s3.bucket=channel-us-order-step-timings
# order.timings.report.s3.region=us-east-1
# order.timings.report.s3.prefix=

# order.timings.report.skipEmail=false
# order.timings.report.skipS3=false
```

| Key | Purpose |
|-----|---------|
| `order.timings.report.store` | BaseStore UID |
| `order.timings.report.hoursBack` | Lookback hours (`1` hourly; `168` for 7-day ad-hoc) |
| `order.timings.report.email.to` / `.cc` | Recipients (`,` or `;`) |
| `order.timings.report.email.from` | Fallback From if mailbox unset |
| `order.timings.report.s3.bucket` / `.region` / `.prefix` | S3 target (`prefix` empty = bucket root) |
| `order.timings.report.skipEmail` / `.skipS3` | Disable a delivery channel |

---

### ImpEx — `core-cbus-order-step-timings-hourly.impex`

Registers scheduling only. **Does not set `Script.content`** so re-import never wipes the Groovy body.

**Prerequisites (from ImpEx header)**

1. Groovy already pasted into Script `coreCbusOrderStepTimingsHourlyScript` (or update via Backoffice/HAC).
2. Report properties optional (Groovy fallbacks).
3. Mail path available on the node that runs the CronJob (`bayerEmailService` / mail sender).

```impex
# Do NOT set Script.content here — Groovy body is maintained separately (avoids wiping it).
INSERT_UPDATE Script; code[unique=true]; scriptType(code)
; coreCbusOrderStepTimingsHourlyScript; GROOVY

INSERT_UPDATE ScriptingJob; code[unique=true]; scriptURI; active
; coreCbusOrderStepTimingsHourlyJob; model://coreCbusOrderStepTimingsHourlyScript; true

INSERT_UPDATE CronJob; code[unique=true]; job(code); sessionUser(uid); sessionLanguage(isocode); sessionCurrency(isocode); singleExecutable; active; logToDatabase; logToFile; logLevelDatabase(code); logLevelFile(code)
; coreCbusOrderStepTimingsHourlyCronJob; coreCbusOrderStepTimingsHourlyJob; admin; en; USD; false; true; true; false; INFO; INFO

# Fire at :05 every hour
INSERT_UPDATE Trigger; cronJob(code)[unique=true]; cronExpression; active
; coreCbusOrderStepTimingsHourlyCronJob; 0 5 * * * ?; true
```

| Item created | Code / value |
|--------------|--------------|
| `Script` | `coreCbusOrderStepTimingsHourlyScript` · type `GROOVY` · content managed outside ImpEx |
| `ScriptingJob` | `coreCbusOrderStepTimingsHourlyJob` · `scriptURI=model://coreCbusOrderStepTimingsHourlyScript` · active |
| `CronJob` | `coreCbusOrderStepTimingsHourlyCronJob` · user `admin` · `en`/`USD` · log to DB INFO · active |
| `Trigger` | cron `0 5 * * * ?` · active (minute **:05** every hour; TZ = server/node) |

**Import order tip:** create/update Script **content** first, then import ImpEx. Re-importing ImpEx alone is safe for Job/Cron/Trigger refreshes.

---

## Implementation details

### 1. Time window

```groovy
windowEnd = now
windowStart = now - HOURS_BACK hours   // default HOURS_BACK=1
```

Filter: `{OrderProcess.creationtime} >= since AND < until` and `{state}=SUCCEEDED`, store `uid = STORE`.

For a 7-day backfill, set `order.timings.report.hoursBack=168` (or run Groovy in HAC with that property / temporary override).

### 2. Process selection query

Joins `OrderProcess` → `Order` → `BaseStore`, selects:

- Process PK, code, definition name, creation/modified times, **`spanId`**, **`traceId`**  
- Order code, order creationtime, `modificationSubmitionTime`  
- Kind: `CREATE` if definition contains `sap-order-process`, else `MODIFY`  
- `sapOrderNumber`: strip `S2-` prefix from order code when present  

### 3. Task / step timing

For each process, load all `ProcessTaskLog` ordered by `startDate`.

**CREATE** (`sendOrderToSAP` log messages):

- Parse `Step N/5 [name] STARTING` / `COMPLETED|FAILED`
- Sum `time taken N ms` into `stepN_apiMs`
- Wall clock from log timestamps → `stepN_wallMs`

**MODIFY** (map task actions → synthetic steps):

| Step slot | Action |
|-----------|--------|
| 1 | `prepayFinancing` |
| 2 | `odataBatchStandard` |
| 3 | `odataBatchSplitBill` |
| 4 | `lcrDiscount` |
| 5 | `brandAndSfDiscount` |

**Cart submit time** (`cartSubmitAt`):

- Prefer `ZZ1_C2C_SAT_TECID_HDR` / `satelliteCreationTechID` ISO timestamp from ERP logs  
- Else CREATE: process creation; MODIFY: `modificationSubmitionTime`

Durations over 1 hour or negative are blanked for `msBetween` (sanity clamp).

### 4. CSV columns

Aligned with the Node exporter. Key fields:

- Identity: `orderCode`, `sapOrderNumber`, `kind`, `processCode`, `processDefinition`, `processState`, `spanId`, `traceId` (from `OrderProcess`)
- Lead-in: `cartSubmitAt`, `orderConvertedAt`, `cartToOrderMs`
- Anchors: `orderCreated`, `modSubmitted`, `processCreated`, `processModified`
- Spans: `commercePersistToBpMs`, `bpTriggerToFirstTaskMs`, `processCreatedToFirstErpMs`, `processWallMs`
- Steps 1–5: `stepN_name`, `stepN_apiMs`, `stepN_wallMs`, `stepN_status`
- Aggregates: `erpApiMsSum`, `sendOrderToSAP_ms`, `odataBatch_ms`, `lcrDiscount_ms`, `brandAndSfDiscount_ms`, `prepayFinancing_ms`, `taskActions`

All duration fields are **milliseconds**.

### 5. Email

- Bean: `bayerEmailService.sendEmail(Email, 'core_cbus', uuid)`
- From: `onedce.general.mailbox`, else `order.timings.report.email.from`
- Subject: `Order step timings — last {N}h — {stamp} UTC` (prefixed `[NON-PROD]` when `env` ≠ `PRD`)
- Attachment: base64 CSV via `EmailAttachment`

### 6. S3 upload

- Credentials: Vault path from `onedce.aws.path` (not stored in report properties)
- Client: `bayerAmazonS3IntService.getOneDCEs3Client(region)` then `putObject`
- Default bucket: `channel-us-order-step-timings`, region `us-east-1`, **empty prefix** (object key = file name only)

Return string shape:

```text
OK rows=N file=... email=... s3=s3://bucket/key window=...
```

---

## Configuration

### Property keys

Set in CCV2 aspect properties or `local.properties`. All have Groovy fallbacks if unset.

| Property | Default | Description |
|----------|---------|-------------|
| `order.timings.report.store` | `core_cbus` | BaseStore UID filter |
| `order.timings.report.hoursBack` | `1` | Lookback hours from job start |
| `order.timings.report.email.to` | `punit.dwivedi.ext@bayer.com` | To list (`,` or `;`) |
| `order.timings.report.email.from` | `donot-reply@channelarrow.com` | Fallback From if mailbox unset |
| `order.timings.report.email.cc` | _(empty)_ | Optional CC list |
| `order.timings.report.skipEmail` | `false` | Skip email delivery |
| `order.timings.report.skipS3` | `false` | Skip S3 upload |
| `order.timings.report.s3.bucket` | `channel-us-order-step-timings` | Target bucket |
| `order.timings.report.s3.region` | `us-east-1` | AWS region |
| `order.timings.report.s3.prefix` | _(empty)_ | Optional key prefix (trailing `/` added if needed) |

### Environment prerequisites (already expected on env)

| Property / bean | Notes |
|-----------------|-------|
| `onedce.aws.path` | Vault secret path (`aws-np` on d1, `aws-prod` on p1) — fields `accessKeyId` / `secretAccessKey` |
| `vault.url` | Vault endpoint |
| `onedce.general.mailbox` | Preferred From address |
| `bayerEmailService` | Must be available on the node that runs the CronJob (background nodes need mail config) |
| `bayerAmazonS3IntService` | Vault-backed S3 client factory |

### Example overrides

```properties
order.timings.report.hoursBack=1
order.timings.report.email.to=ops@bayer.com,owner@bayer.com
order.timings.report.email.cc=team@bayer.com
order.timings.report.s3.bucket=channel-us-order-step-timings
order.timings.report.s3.region=us-east-1
order.timings.report.s3.prefix=
```

---

## How to deploy / configure

### First-time setup

1. **Create / update Script content** in HAC Scripting or Backoffice:
   - Code: `coreCbusOrderStepTimingsHourlyScript`
   - Type: `GROOVY`
   - Paste contents of `scripts/hybris/core-cbus-order-step-timings-hourly.groovy`
   - Keep **one** Script row for that code (duplicates make `model://` resolution ambiguous)

2. **Import ImpEx** (`scripts/hybris/core-cbus-order-step-timings-hourly.impex`) to create Job / CronJob / Trigger.  
   ImpEx does **not** set `Script.content` (avoids wiping the Groovy body).

3. Confirm CronJob **active**, Trigger **active**, expression `0 5 * * * ?`.

4. Optionally set properties above in the CCV2 aspect; otherwise defaults apply.

### Update script logic later

1. Edit `scripts/hybris/core-cbus-order-step-timings-hourly.groovy` in git.
2. Replace Script **content** on the env (HAC / Backoffice / Groovy update).  
   Do **not** re-import content via ImpEx `INSERT_UPDATE Script; ...; content`.
3. Re-run CronJob once or wait for the next `:05` fire.

### Manual / ad-hoc runs

- **Hourly window**: trigger CronJob `coreCbusOrderStepTimingsHourlyCronJob` or execute the Script in HAC.
- **Longer window** (e.g. 7 days): temporarily set `order.timings.report.hoursBack=168`, run once, then restore to `1`.
- **Email or S3 only**: `skipEmail=true` or `skipS3=true`.

### Verify

| Check | Where |
|-------|--------|
| Cron last/next fire | Backoffice CronJob |
| Log line `Order timings report starting` / `OK rows=` | App logs, logger `coreCbusOrderStepTimingsHourly` |
| Inbox | Subject `Order step timings — last Nh — … UTC` + CSV attachment |
| S3 object | `s3://channel-us-order-step-timings/core_cbus-order-step-timings-*.csv` |

---

## Operational notes

- **Duplicate Scripts**: multiple `Script` items with the same `code` break reliable `model://` resolution. Keep a single active row; remove extras.
- **Empty window**: `rows=0` is valid when no SUCCEEDED processes were created in the lookback; email/S3 still run with header-only CSV.
- **BG node mail**: if email fails with missing `mailSender` / JavaMail beans, ensure the CronJob runs on a node with mail configured (or use `skipEmail` and rely on S3).
- **S3 AccessDenied**: report uses dedicated bucket `channel-us-order-step-timings`; Vault IAM must allow `s3:PutObject` there.
- **Timezone**: file stamp and email window labels use **UTC**; Cron expression follows the server/node timezone for “:05”.

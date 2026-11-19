/**
 * Hourly / market-year backfill cbus order step-timings report (CREATE + MODIFY).
 *
 * Modes (order.timings.report.mode):
 *   hourly   — past N hours of SUCCEEDED OrderProcess rows (default; cron-friendly)
 *   backfill — regenerate CSVs from market-year start (1 Sep) through now (or explicit range)
 *
 * Market year: 1 Sep (year Y) → 31 Aug (year Y+1). If "now" is before 1 Sep, Y = previous calendar year.
 *
 * Outputs CSV (timing columns + lineItemCount + prior API-failure columns), emails (hourly),
 * and uploads each file to S3 as cbus-order-step-timings-YYYYMMDD-HHMM.csv.
 *
 * Prior-failure enrichment (batched, not N+1):
 *   - FAILED/ERROR sibling OrderProcess rows for the same order (before this process)
 *   - BayerOrderChainProcessStep rows with status=FAILURE on the same order
 * Optional lookback bounds history scans (default 30 days before each chunk end).
 *
 * Backfill tips (avoid HAC timeouts):
 *   - Processes in chunkHours windows (default 24 = one UTC day)
 *   - Set backfill.maxChunks (e.g. 7) and re-run; advance backfill.from / resumeFrom each run
 *   - Email is skipped by default in backfill; S3 upload per non-empty chunk
 *
 * Schedule via ScriptingJob + CronJob (see companion ImpEx).
 *
 * Optional hybris config (local.properties / CCV2 aspect) — hardcoded fallbacks:
 *   order.timings.report.store=cbus
 *   order.timings.report.mode=hourly
 *   order.timings.report.hoursBack=1
 *   order.timings.report.backfill.from=2026-09-01   (optional; default = market year start UTC)
 *   order.timings.report.backfill.to=                 (optional; default = now)
 *   order.timings.report.backfill.resumeFrom=         (optional; inclusive restart date YYYY-MM-DD)
 *   order.timings.report.backfill.chunkHours=24
 *   order.timings.report.backfill.maxChunks=0         (0 = no limit)
 *   order.timings.report.backfill.uploadEmpty=false
 *   order.timings.report.backfill.skipEmail=true
 *   order.timings.report.priorFailure.lookbackDays=30
 *   order.timings.report.priorFailure.messageMaxLen=400
 *   order.timings.report.pkChunkSize=300
 *   order.timings.report.email.to=a@bayer.com,b@bayer.com  (comma or semicolon list)
 *   order.timings.report.email.from=donot-reply@channelarrow.com
 *   order.timings.report.email.cc=   (optional; same list format)
 *   order.timings.report.skipEmail=false
 *   order.timings.report.skipS3=false
 *   order.timings.report.s3.bucket=   (optional override; default from env — see below)
 *   order.timings.report.s3.region=us-east-1
 *   order.timings.report.s3.prefix=   (empty = upload at bucket root)
 *
 * S3 bucket auto-select (Config key env) — same as working core_cbus script:
 *   PRD  → channel-us-order-step-timings        (hybris-p1)
 *   else → channel-us-order-step-timings-np     (hybris-d1 / non-prod)
 * Explicit order.timings.report.s3.bucket always wins when set.
 *
 * S3 credentials: NOT stored in properties. Uploads go through bean
 * bayerAmazonS3Service → BayerAmazonS3IntServiceImpl.getOneDCEs3Client(),
 * which reads Vault secret at Config key onedce.aws.path
 * (d1: secret/explorers_onedce/aws-np, p1: .../aws-prod) and uses vault fields
 * accessKeyId / secretAccessKey. vault.url must already be configured.
 *
 * Email: same path as coreUsOrderMonitorScript —
 *   bayerEmailService + com.bayer.integration.Email (+ EmailAttachment base64).
 *   From address defaults to onedce.general.mailbox.
 */

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.model.ObjectMetadata
import com.bayer.integration.Email
import com.bayer.integration.EmailAttachment
import de.hybris.platform.servicelayer.search.FlexibleSearchQuery
import de.hybris.platform.processengine.enums.ProcessState
import de.hybris.platform.core.PK
import de.hybris.platform.util.Config

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat
import java.util.Base64
import java.util.TimeZone
import java.util.UUID
import java.util.regex.Pattern

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------
def cfg = { String key, String defVal = '' ->
    def v = Config.getParameter(key)
    (v == null || v.trim().isEmpty()) ? defVal : v.trim()
}
def cfgBool = { String key, boolean defVal ->
    def v = cfg(key, String.valueOf(defVal))
    'true'.equalsIgnoreCase(v) || '1'.equals(v) || 'yes'.equalsIgnoreCase(v)
}
/** Parse comma- and/or semicolon-separated email list into distinct trimmed addresses. */
def parseEmailList = { String raw ->
    if (!raw) return []
    raw.split('[,;]')
        .collect { it?.trim() }
        .findAll { it }
        .unique()
}

// Dedicated report buckets — same resolution as working core_cbus groovy
final String S3_BUCKET_PROD = 'channel-us-order-step-timings'
final String S3_BUCKET_NONPROD = 'channel-us-order-step-timings-np'
final String DEFAULT_S3_REGION = 'us-east-1'
final String DEFAULT_S3_PREFIX = ''

// --- One-off HAC overrides (non-null / non-empty wins over Config + defaults) ---
// Monthly/backfill: set OVERRIDE_MODE='backfill' + from/to. For hourly cron, set OVERRIDE_MODE=null.
// Batches of 7 UTC days — re-run with OVERRIDE_RESUME_FROM = nextResumeFrom from prior summary.
final String OVERRIDE_MODE = null
final String OVERRIDE_BACKFILL_FROM = null
final String OVERRIDE_BACKFILL_TO = null
final String OVERRIDE_RESUME_FROM = null
final Integer OVERRIDE_MAX_CHUNKS = null
final Integer OVERRIDE_CHUNK_HOURS = null
final String OVERRIDE_S3_BUCKET = null

final String STORE = cfg('order.timings.report.store', 'cbus')
final String MODE = (OVERRIDE_MODE ?: cfg('order.timings.report.mode', 'hourly')).toLowerCase()
final int HOURS_BACK = Integer.parseInt(cfg('order.timings.report.hoursBack', '1'))
final int PRIOR_LOOKBACK_DAYS = Integer.parseInt(cfg('order.timings.report.priorFailure.lookbackDays', '30'))
final int PRIOR_MSG_MAX_LEN = Integer.parseInt(cfg('order.timings.report.priorFailure.messageMaxLen', '400'))
final int PK_CHUNK_SIZE = Integer.parseInt(cfg('order.timings.report.pkChunkSize', '300'))
final int BACKFILL_CHUNK_HOURS = OVERRIDE_CHUNK_HOURS != null
    ? OVERRIDE_CHUNK_HOURS.intValue()
    : Integer.parseInt(cfg('order.timings.report.backfill.chunkHours', '24'))
final int BACKFILL_MAX_CHUNKS = OVERRIDE_MAX_CHUNKS != null
    ? OVERRIDE_MAX_CHUNKS.intValue()
    : Integer.parseInt(cfg('order.timings.report.backfill.maxChunks', '0'))
final boolean BACKFILL_UPLOAD_EMPTY = cfgBool('order.timings.report.backfill.uploadEmpty', false)
final boolean BACKFILL_SKIP_EMAIL = cfgBool('order.timings.report.backfill.skipEmail', true)
final String BACKFILL_FROM_CFG = OVERRIDE_BACKFILL_FROM ?: cfg('order.timings.report.backfill.from', '')
final String BACKFILL_TO_CFG = OVERRIDE_BACKFILL_TO ?: cfg('order.timings.report.backfill.to', '')
final String BACKFILL_RESUME_CFG = OVERRIDE_RESUME_FROM ?: cfg('order.timings.report.backfill.resumeFrom', '')
final List EMAIL_TO = parseEmailList(cfg('order.timings.report.email.to', 'punit.dwivedi.ext@bayer.com'))
final String EMAIL_FROM = cfg('order.timings.report.email.from', 'donot-reply@channelarrow.com')
final List EMAIL_CC = parseEmailList(cfg('order.timings.report.email.cc', ''))

// env=PRD → prod bucket (p1); anything else (DEV/STG/d1/s1/blank) → non-prod (-np)
final String HYBRIS_ENV = cfg('env', 'DEV')
final boolean IS_PROD = 'PRD'.equalsIgnoreCase(HYBRIS_ENV)
final String DEFAULT_S3_BUCKET = IS_PROD ? S3_BUCKET_PROD : S3_BUCKET_NONPROD
final String S3_BUCKET_CFG = cfg('order.timings.report.s3.bucket', '')
final String S3_BUCKET = OVERRIDE_S3_BUCKET ?: (S3_BUCKET_CFG ?: DEFAULT_S3_BUCKET)
final String S3_REGION = cfg('order.timings.report.s3.region', DEFAULT_S3_REGION)
final String S3_PREFIX = cfg('order.timings.report.s3.prefix', DEFAULT_S3_PREFIX)
final String VAULT_AWS_PATH = cfg('onedce.aws.path', '')
final boolean SKIP_EMAIL = cfgBool('order.timings.report.skipEmail', false)
final boolean SKIP_S3 = cfgBool('order.timings.report.skipS3', false)

def fss = spring.getBean('flexibleSearchService')
def log = org.slf4j.LoggerFactory.getLogger('cbusOrderStepTimingsHourly')

log.info(
    "S3 bucket resolved env={} isProd={} bucket={} (override={} config={})",
    HYBRIS_ENV, IS_PROD, S3_BUCKET, OVERRIDE_S3_BUCKET ?: '', S3_BUCKET_CFG ?: ''
)

final TimeZone UTC = TimeZone.getTimeZone('UTC')
def isoUtc = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
isoUtc.setTimeZone(UTC)
def fileStamp = new SimpleDateFormat('yyyyMMdd-HHmm')
fileStamp.setTimeZone(UTC)
def dayStamp = new SimpleDateFormat('yyyy-MM-dd')
dayStamp.setTimeZone(UTC)

// ---------------------------------------------------------------------------
// CSV helpers (aligned with Node export)
// ---------------------------------------------------------------------------
final List HEADER = [
    'orderCode', 'sapOrderNumber', 'agentAccount', 'agentName',
    'kind', 'processCode', 'processDefinition', 'processState', 'spanId', 'traceId',
    'lineItemCount',
    'cartSubmitAt', 'orderConvertedAt', 'cartToOrderMs',
    'orderCreated', 'modSubmitted', 'processCreated', 'processModified',
    'commercePersistToBpMs', 'bpTriggerToFirstTaskMs', 'processCreatedToFirstErpMs', 'processWallMs',
    'step1_name', 'step1_apiMs', 'step1_wallMs', 'step1_status',
    'step2_name', 'step2_apiMs', 'step2_wallMs', 'step2_status',
    'step3_name', 'step3_apiMs', 'step3_wallMs', 'step3_status',
    'step4_name', 'step4_apiMs', 'step4_wallMs', 'step4_status',
    'step5_name', 'step5_apiMs', 'step5_wallMs', 'step5_status',
    'erpApiMsSum', 'sendOrderToSAP_ms', 'odataBatch_ms', 'lcrDiscount_ms',
    'brandAndSfDiscount_ms', 'prepayFinancing_ms', 'taskActions',
    'hadPriorApiFailure', 'priorApiFailureAt', 'priorApiFailureCount',
    'priorApiFailureSource', 'priorApiFailureStep', 'priorApiFailureMessage'
]

def csvEscape = { Object v ->
    if (v == null) return ''
    String s = String.valueOf(v)
    if (s.isEmpty()) return ''
    if (s.contains('"') || s.contains(',') || s.contains('\n') || s.contains('\r')) {
        return '"' + s.replace('"', '""') + '"'
    }
    return s
}

def parseIsoDate = { String s ->
    if (!s) return null
    try {
        String norm = s.trim()
        if (norm.endsWith('Z')) {
            return Date.from(java.time.Instant.parse(norm))
        }
        if (norm.length() >= 19 && norm.charAt(10) == ('T' as char)) {
            return Date.from(java.time.OffsetDateTime.parse(norm).toInstant())
        }
    } catch (ignored) {}
    try {
        def f = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
        return f.parse(s)
    } catch (ignored) {}
    try {
        def f = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        f.setTimeZone(TimeZone.getTimeZone('UTC'))
        return f.parse(s)
    } catch (ignored) {}
    null
}

def parseHybrisDate = { Object o ->
    if (o == null) return null
    if (o instanceof Date) return (Date) o
    String s = String.valueOf(o).trim()
    if (s.isEmpty()) return null
    if (s.contains('T')) {
        Date iso = parseIsoDate(s)
        if (iso != null) return iso
    }
    try {
        return new SimpleDateFormat('EEE MMM dd HH:mm:ss zzz yyyy', Locale.ENGLISH).parse(s)
    } catch (ignored) {}
    try {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(s)
    } catch (ignored) {}
    null
}

def formatTs = { Date d -> d == null ? '' : isoUtc.format(d) }

def truncateMsg = { Object raw, int maxLen = PRIOR_MSG_MAX_LEN ->
    if (raw == null) return ''
    String t = String.valueOf(raw).replaceAll(/\s+/, ' ').trim()
    if (t.isEmpty()) return ''
    t.length() <= maxLen ? t : t.substring(0, maxLen)
}

def chunkList = { List items, int size ->
    if (!items) return []
    int n = Math.max(1, size)
    def chunks = []
    for (int i = 0; i < items.size(); i += n) {
        chunks << items.subList(i, Math.min(i + n, items.size()))
    }
    chunks
}

def msBetween = { Date a, Date b ->
    if (a == null || b == null) return ''
    long ms = b.time - a.time
    if (ms < 0 || Math.abs(ms) > 3_600_000L) return ''
    String.valueOf(ms)
}

def msDur = { Date a, Date b ->
    if (a == null || b == null) return ''
    String.valueOf(Math.max(0L, b.time - a.time))
}

def parseCartSubmitAt = { String msg ->
    if (!msg) return null
    def m = (msg =~ /ZZ1_C2C_SAT_TECID_HDR[^0-9]*([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.+-]+)/)
    if (!m.find()) {
        m = (msg =~ /(?i)satelliteCreationTechID[^0-9]*([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.+-]+)/)
        if (!m.find()) return null
    }
    parseIsoDate(m.group(1))
}

def todMs = { String t ->
    if (!t) return null
    def parts = t.split(':')
    if (parts.length < 3) return null
    def secMs = parts[2].split('\\.')
    int h = parts[0] as int
    int m = parts[1] as int
    int s = secMs[0] as int
    int ms = secMs.length > 1 ? (secMs[1] as int) : 0
    h * 3600_000L + m * 60_000L + s * 1000L + ms
}

def parseCreateSteps = { String logMsg ->
    def stepNames = ['', '', '', '', '']
    def stepApi = ['', '', '', '', '']
    def stepWall = ['', '', '', '', '']
    def stepStatus = ['', '', '', '', '']
    long erpApiMsSum = 0
    if (!logMsg) {
        return [stepNames: stepNames, stepApi: stepApi, stepWall: stepWall, stepStatus: stepStatus, erpApiMsSum: erpApiMsSum]
    }
    Integer currentStep = null
    def apiByStep = [:]
    def nameByStep = [:]
    def statusByStep = [:]
    def startTs = [:]
    def endTs = [:]
    def tsRe = Pattern.compile(/(\d{2}:\d{2}:\d{2}\.\d{3})/)

    logMsg.readLines().each { line ->
        def tsM = tsRe.matcher(line)
        String ts = tsM.find() ? tsM.group(1) : null
        def stM = (line =~ /(?i)Step\s+(\d+)\/5\s+\[([^\]]+)\].*STARTING/)
        if (stM.find()) {
            int n = stM.group(1) as int
            currentStep = n
            nameByStep[n] = stM.group(2)
            if (ts) startTs[n] = ts
        }
        def dnM = (line =~ /(?i)Step\s+(\d+)\s+\[([^\]]+)\].*(COMPLETED SUCCESSFULLY|FAILED)/)
        if (dnM.find()) {
            int n = dnM.group(1) as int
            statusByStep[n] = dnM.group(3).toUpperCase().startsWith('COMPLETED') ? 'SUCCESS' : 'FAILED'
            nameByStep[n] = nameByStep[n] ?: dnM.group(2)
            if (ts) endTs[n] = ts
        }
        def tmM = (line =~ /(?i)time taken\s+(\d+)\s+ms/)
        if (tmM.find() && currentStep != null) {
            apiByStep[currentStep] = (apiByStep[currentStep] ?: 0L) + (tmM.group(1) as long)
        }
    }
    (1..5).each { int n ->
        stepNames[n - 1] = nameByStep[n] ?: ''
        if (apiByStep[n] != null) {
            stepApi[n - 1] = String.valueOf(apiByStep[n])
            erpApiMsSum += apiByStep[n]
        }
        stepStatus[n - 1] = statusByStep[n] ?: ''
        Long a = todMs(startTs[n] as String)
        Long b = todMs(endTs[n] as String)
        if (a != null && b != null) {
            long d = b - a
            if (d < 0) d += 24L * 3600_000L
            stepWall[n - 1] = String.valueOf(d)
        }
    }
    [stepNames: stepNames, stepApi: stepApi, stepWall: stepWall, stepStatus: stepStatus, erpApiMsSum: erpApiMsSum]
}

def keepActions = [
    'sendOrderToSAP', 'odataBatchStandard', 'odataBatchSplitBill',
    'lcrDiscount', 'brandAndSfDiscount', 'prepayFinancing'
] as Set

def compactLog = { String msg ->
    if (!msg) return ''
    def lines = msg.readLines().findAll { line ->
        line.contains('Step ') || line.contains('time taken') || line.contains('OrderCreationChain') ||
            line.contains('STARTING') || line.contains('COMPLETED') || line.contains('FAILED') ||
            line.contains('Calling S4 API') || line.contains('Calling SalesForce API') ||
            line.contains('SAT_TECID') || line.contains('satelliteCreation') ||
            line.contains('executing for order')
    }
    String c = lines.join('\n')
    c.length() > 14000 ? c.substring(0, 14000) : c
}

def loadTasks = { PK processPk ->
    def lq = new FlexibleSearchQuery(
        'SELECT {actionId},{startDate},{endDate},{returnCode},{logMessages} FROM {ProcessTaskLog} WHERE {process}=?p ORDER BY {startDate} ASC'
    )
    lq.addQueryParameter('p', processPk)
    lq.setResultClassList([String.class, Date.class, Date.class, String.class, String.class])
    fss.search(lq).result.collect { tl ->
        String action = tl[0] ?: ''
        String msg = ''
        if (keepActions.contains(action) && tl[4]) {
            msg = compactLog(String.valueOf(tl[4]))
        }
        [
            action: action,
            start : tl[1] as Date,
            end   : tl[2] as Date,
            rc    : tl[3] ?: '',
            msg   : msg
        ]
    }
}

def loadOrderEnrichment = { List orderPks, Date lookbackSince ->
    Map lineItemCountByOrder = [:] as Map
    Map failuresByOrder = [:] as Map

    if (!orderPks) {
        return [lineItemCountByOrder: lineItemCountByOrder, failuresByOrder: failuresByOrder]
    }

    List distinctOrders = orderPks.unique(false)
    def failedStates = [ProcessState.FAILED, ProcessState.ERROR]
    Map nokByFailedProcess = [:] as Map
    List failedProcessRows = []

    chunkList(distinctOrders, PK_CHUNK_SIZE).each { List chunk ->
        def eq = new FlexibleSearchQuery('''
SELECT {e.order}, COUNT({e.pk})
FROM {OrderEntry AS e}
WHERE {e.order} IN (?orders)
GROUP BY {e.order}
''')
        eq.addQueryParameter('orders', chunk)
        eq.setResultClassList([PK.class, Long.class])
        eq.setNeedTotal(false)
        fss.search(eq).result.each { row ->
            lineItemCountByOrder[String.valueOf(row[0])] = (row[1] as Long).intValue()
        }

        def fq = new FlexibleSearchQuery('''
SELECT {op.order}, {op.pk}, {op.code}, {op.processDefinitionName}, {op.state}, {op.creationtime}, {op.modifiedtime}
FROM {OrderProcess AS op}
WHERE {op.order} IN (?orders) AND {op.state} IN (?failed) AND {op.creationtime}>=?lookback
''')
        fq.addQueryParameter('orders', chunk)
        fq.addQueryParameter('failed', failedStates)
        fq.addQueryParameter('lookback', lookbackSince)
        fq.setResultClassList([PK.class, PK.class, String.class, String.class, ProcessState.class, Date.class, Date.class])
        fq.setNeedTotal(false)
        fss.search(fq).result.each { row ->
            failedProcessRows << [
                orderPk    : row[0] as PK,
                processPk  : row[1] as PK,
                processCode: row[2] ?: '',
                defName    : row[3] ?: '',
                state      : row[4] != null ? String.valueOf(row[4]) : '',
                created    : row[5] as Date,
                modified   : row[6] as Date
            ]
        }

        def cq = new FlexibleSearchQuery('''
SELECT {cs.order}, {st.actionId}, {st.stepName}, {st.startTime}, {st.endTime}, {st.errorMessage},
       {cs.businessProcessId}, {cs.creationtime}
FROM {BayerOrderChainProcessStep AS st
      JOIN BayerOrderChainProcessStatus AS cs ON {st.orderChainProcessStatus}={cs.pk}}
WHERE {cs.order} IN (?orders) AND {st.status}=?fail AND {cs.creationtime}>=?lookback
''')
        cq.addQueryParameter('orders', chunk)
        cq.addQueryParameter('fail', 'FAILURE')
        cq.addQueryParameter('lookback', lookbackSince)
        cq.setResultClassList([
            PK.class, String.class, String.class, Date.class, Date.class, String.class, String.class, Date.class
        ])
        cq.setNeedTotal(false)
        fss.search(cq).result.each { row ->
            String orderKey = String.valueOf(row[0])
            Date at = (row[3] as Date) ?: (row[4] as Date) ?: (row[7] as Date)
            String step = (row[2] ?: row[1] ?: '') as String
            def event = [
                source : 'CHAIN',
                at     : at,
                step   : step,
                message: truncateMsg(row[5]),
                processCode: row[6] ?: ''
            ]
            failuresByOrder.computeIfAbsent(orderKey, { k -> [] }) << event
        }
    }

    List failedProcPks = failedProcessRows.collect { it.processPk }.unique(false)
    chunkList(failedProcPks, PK_CHUNK_SIZE).each { List chunk ->
        if (!chunk) return
        def tq = new FlexibleSearchQuery('''
SELECT {tl.process}, {tl.actionId}, {tl.startDate}, {tl.endDate}, {tl.returnCode}, {tl.logMessages}
FROM {ProcessTaskLog AS tl}
WHERE {tl.process} IN (?procs) AND {tl.returnCode}=?nok
ORDER BY {tl.startDate} DESC
''')
        tq.addQueryParameter('procs', chunk)
        tq.addQueryParameter('nok', 'NOK')
        tq.setResultClassList([PK.class, String.class, Date.class, Date.class, String.class, String.class])
        tq.setNeedTotal(false)
        fss.search(tq).result.each { row ->
            String procKey = String.valueOf(row[0])
            if (nokByFailedProcess.containsKey(procKey)) return
            nokByFailedProcess[procKey] = [
                step   : row[1] ?: '',
                at     : (row[2] as Date) ?: (row[3] as Date),
                message: truncateMsg(row[5])
            ]
        }
    }

    failedProcessRows.each { fp ->
        String orderKey = String.valueOf(fp.orderPk)
        String procKey = String.valueOf(fp.processPk)
        def nok = nokByFailedProcess[procKey]
        Date at = (nok?.at as Date) ?: (fp.modified as Date) ?: (fp.created as Date)
        String step = (nok?.step ?: fp.defName ?: fp.state ?: '') as String
        String message = (nok?.message ?: "OrderProcess ${fp.state}: ${fp.processCode}") as String
        def event = [
            source     : 'BP',
            at         : at,
            step       : step,
            message    : truncateMsg(message),
            processCode: fp.processCode ?: '',
            processPk  : fp.processPk,
            created    : fp.created
        ]
        failuresByOrder.computeIfAbsent(orderKey, { k -> [] }) << event
    }

    log.info(
        "Enrichment loaded orders={} lineItemKeys={} failureOrderKeys={} failedBpRows={} nokTaskLogs={}",
        distinctOrders.size(), lineItemCountByOrder.size(), failuresByOrder.size(),
        failedProcessRows.size(), nokByFailedProcess.size()
    )
    [lineItemCountByOrder: lineItemCountByOrder, failuresByOrder: failuresByOrder]
}

def summarizePriorFailures = { Map proc, Map failuresByOrder ->
    String orderKey = String.valueOf(proc.orderPk)
    Date cutoff = proc.procCreated as Date
    PK selfPk = proc.pk as PK
    List events = (failuresByOrder[orderKey] ?: []) as List
    List prior = events.findAll { ev ->
        if (cutoff == null) return false
        if (ev.source == 'BP' && ev.processPk != null && selfPk != null && ev.processPk == selfPk) return false
        if (ev.source == 'BP') {
            Date created = ev.created as Date
            return created != null && created.before(cutoff)
        }
        Date when = (ev.at as Date) ?: (ev.created as Date)
        when != null && when.before(cutoff)
    }
    if (!prior) {
        return [
            hadPriorApiFailure     : 'false',
            priorApiFailureAt      : '',
            priorApiFailureCount   : '0',
            priorApiFailureSource  : '',
            priorApiFailureStep    : '',
            priorApiFailureMessage : ''
        ]
    }
    def latest = prior.max { ev ->
        Date when = (ev.at as Date) ?: (ev.created as Date)
        when ? when.time : 0L
    }
    [
        hadPriorApiFailure     : 'true',
        priorApiFailureAt      : formatTs(latest.at as Date ?: latest.created as Date),
        priorApiFailureCount   : String.valueOf(prior.size()),
        priorApiFailureSource  : latest.source ?: '',
        priorApiFailureStep    : latest.step ?: '',
        priorApiFailureMessage : latest.message ?: ''
    ]
}

def buildRow = { Map proc, List tasks, Map enrichment ->
    boolean isCreate = proc.kind == 'CREATE'
    Date firstTask = null
    Date lastTask = null
    Date firstErp = null
    def actionDurs = [:] as Map
    def apiMsByAction = [:] as Map
    def taskNames = []
    String sendMsg = ''
    StringBuilder allErpMsg = new StringBuilder()

    (tasks ?: []).each { t ->
        taskNames << "${t.action}:${t.rc}"
        Date s = t.start instanceof Date ? t.start : parseHybrisDate(t.start)
        Date e = t.end instanceof Date ? t.end : parseHybrisDate(t.end)
        if (s && (!firstTask || s.before(firstTask))) firstTask = s
        if (e && (!lastTask || e.after(lastTask))) lastTask = e
        if (s && e) {
            actionDurs[t.action] = (actionDurs[t.action] ?: 0L) + (e.time - s.time)
        }
        boolean erpish = keepActions.contains(t.action)
        if (erpish && s && (!firstErp || s.before(firstErp))) firstErp = s
        if (t.msg) {
            allErpMsg.append('\n').append(t.msg)
            long sum = 0
            def mm = (t.msg =~ /(?i)time taken\s+(\d+)\s+ms/)
            while (mm.find()) sum += mm.group(1) as long
            if (sum > 0) apiMsByAction[t.action] = (apiMsByAction[t.action] ?: 0L) + sum
        }
        if (t.action == 'sendOrderToSAP' && t.msg) sendMsg = t.msg
    }

    def stepNames = ['', '', '', '', '']
    def stepApi = ['', '', '', '', '']
    def stepWall = ['', '', '', '', '']
    def stepStatus = ['', '', '', '', '']
    long erpApiMsSum = 0

    if (isCreate) {
        def parsed = parseCreateSteps(sendMsg)
        stepNames = parsed.stepNames
        stepApi = parsed.stepApi
        stepWall = parsed.stepWall
        stepStatus = parsed.stepStatus
        erpApiMsSum = parsed.erpApiMsSum as long
    } else {
        def mapping = [
            ['prepayFinancing', 'Prepay / Brand Financing'],
            ['odataBatchStandard', 'OData Batch Modify (Sales Order)'],
            ['odataBatchSplitBill', 'OData Batch Split Bill'],
            ['lcrDiscount', 'LCR Discount'],
            ['brandAndSfDiscount', 'Brand & SF Discount']
        ]
        mapping.eachWithIndex { pair, idx ->
            String action = pair[0]
            String name = pair[1]
            long durMs = (actionDurs[action] ?: 0L) as long
            long api = (apiMsByAction[action] ?: 0L) as long
            if (durMs > 0 || api > 0) {
                stepNames[idx] = name
                stepWall[idx] = durMs > 0 ? String.valueOf(durMs) : ''
                stepApi[idx] = api > 0 ? String.valueOf(api) : ''
                stepStatus[idx] = 'SUCCESS'
                erpApiMsSum += api
            }
        }
    }

    Date orderCreated = parseHybrisDate(proc.orderCreated)
    Date modSubmitted = parseHybrisDate(proc.modSubmitted)
    Date procCreated = parseHybrisDate(proc.procCreated)

    Date satFromLog = parseCartSubmitAt(sendMsg) ?: parseCartSubmitAt(allErpMsg.toString())
    Date cartSubmitAt = isCreate ? (satFromLog ?: procCreated) : (modSubmitted ?: satFromLog ?: procCreated)
    Date orderConvertedAt = procCreated ?: orderCreated
    String cartToOrderMs = msBetween(cartSubmitAt, orderConvertedAt)
    Date anchor = isCreate ? (cartSubmitAt ?: orderCreated) : (modSubmitted ?: cartSubmitAt ?: orderCreated)
    long odataMs = ((actionDurs.odataBatchStandard ?: 0L) as long) + ((actionDurs.odataBatchSplitBill ?: 0L) as long)

    String orderKey = String.valueOf(proc.orderPk)
    Integer lineItemCount = enrichment.lineItemCountByOrder[orderKey] as Integer
    Map prior = summarizePriorFailures(proc, enrichment.failuresByOrder as Map)

    [
        proc.orderCode, proc.sapOrderNumber, proc.agentAccount ?: '', proc.agentName ?: '',
        proc.kind, proc.processCode, proc.defName, proc.state, proc.spanId, proc.traceId,
        lineItemCount != null ? String.valueOf(lineItemCount) : '0',
        formatTs(cartSubmitAt), formatTs(orderConvertedAt), cartToOrderMs,
        proc.orderCreatedStr, proc.modSubmittedStr, proc.procCreatedStr, proc.procModifiedStr,
        msBetween(anchor, procCreated),
        msDur(procCreated, firstTask),
        msDur(procCreated, firstErp),
        msDur(firstTask, lastTask),
        stepNames[0], stepApi[0], stepWall[0], stepStatus[0],
        stepNames[1], stepApi[1], stepWall[1], stepStatus[1],
        stepNames[2], stepApi[2], stepWall[2], stepStatus[2],
        stepNames[3], stepApi[3], stepWall[3], stepStatus[3],
        stepNames[4], stepApi[4], stepWall[4], stepStatus[4],
        erpApiMsSum > 0 ? String.valueOf(erpApiMsSum) : '',
        actionDurs.sendOrderToSAP ? String.valueOf(actionDurs.sendOrderToSAP as long) : '',
        odataMs > 0 ? String.valueOf(odataMs) : '',
        actionDurs.lcrDiscount ? String.valueOf(actionDurs.lcrDiscount as long) : '',
        actionDurs.brandAndSfDiscount ? String.valueOf(actionDurs.brandAndSfDiscount as long) : '',
        actionDurs.prepayFinancing ? String.valueOf(actionDurs.prepayFinancing as long) : '',
        taskNames.join('|'),
        prior.hadPriorApiFailure, prior.priorApiFailureAt, prior.priorApiFailureCount,
        prior.priorApiFailureSource, prior.priorApiFailureStep, prior.priorApiFailureMessage
    ].collect { csvEscape(it) }.join(',')
}

def parseConfigDate = { String raw, boolean endOfDay = false ->
    if (!raw) return null
    String s = raw.trim()
    if (s.length() == 10 && s.charAt(4) == ('-' as char) && s.charAt(7) == ('-' as char)) {
        Calendar c = Calendar.getInstance(UTC)
        c.clear()
        c.set(Calendar.YEAR, Integer.parseInt(s.substring(0, 4)))
        c.set(Calendar.MONTH, Integer.parseInt(s.substring(5, 7)) - 1)
        c.set(Calendar.DAY_OF_MONTH, Integer.parseInt(s.substring(8, 10)))
        if (endOfDay) {
            c.add(Calendar.DAY_OF_MONTH, 1)
        }
        return c.getTime()
    }
    Date iso = parseIsoDate(s)
    if (iso != null) return iso
    throw new IllegalArgumentException("Unrecognized date config value: ${raw}")
}

def marketYearStartUtc = { Date asOf ->
    Calendar c = Calendar.getInstance(UTC)
    c.setTime(asOf)
    int y = c.get(Calendar.YEAR)
    Calendar sep1 = Calendar.getInstance(UTC)
    sep1.clear()
    sep1.set(Calendar.YEAR, y)
    sep1.set(Calendar.MONTH, Calendar.SEPTEMBER)
    sep1.set(Calendar.DAY_OF_MONTH, 1)
    if (asOf.before(sep1.getTime())) {
        sep1.set(Calendar.YEAR, y - 1)
    }
    sep1.getTime()
}

def marketYearEndExclusiveUtc = { Date myStart ->
    Calendar c = Calendar.getInstance(UTC)
    c.setTime(myStart)
    c.add(Calendar.YEAR, 1)
    c.getTime()
}

def buildCsvForWindow = { Date wSince, Date wUntil ->
    Calendar lookbackCal = Calendar.getInstance(UTC)
    lookbackCal.setTime(wUntil)
    lookbackCal.add(Calendar.DAY_OF_YEAR, -Math.max(1, PRIOR_LOOKBACK_DAYS))
    Date priorLookbackSince = lookbackCal.getTime()

    String chunkFileName = "cbus-order-step-timings-${fileStamp.format(wUntil)}.csv"
    String chunkLabel = "${isoUtc.format(wSince)} → ${isoUtc.format(wUntil)} (UTC)"

    def listQ = new FlexibleSearchQuery('''
SELECT {op.pk}, {op.code}, {op.processDefinitionName}, {op.creationtime}, {op.modifiedtime}, {op.spanId}, {op.traceId},
       {o.pk}, {o.code}, {o.creationtime}, {o.modificationSubmitionTime},
       {a.sapAccountId}, {a.name}
FROM {OrderProcess AS op
      JOIN Order AS o ON {op.order}={o.pk}
      JOIN BaseStore AS s ON {o.store}={s.pk}
      LEFT JOIN MonsantoB2BUnit AS a ON {o.agent}={a.pk}}
WHERE {s.uid}=?store AND {op.creationtime}>=?since AND {op.creationtime}<?until AND {op.state}=?st
ORDER BY {op.creationtime} ASC
''')
    listQ.addQueryParameter('store', STORE)
    listQ.addQueryParameter('since', wSince)
    listQ.addQueryParameter('until', wUntil)
    listQ.addQueryParameter('st', ProcessState.SUCCEEDED)
    listQ.setResultClassList([
        PK.class, String.class, String.class, Date.class, Date.class, String.class, String.class,
        PK.class, String.class, Date.class, Date.class,
        String.class, String.class
    ])
    listQ.setNeedTotal(false)

    def processes = fss.search(listQ).result
    log.info("Window {} — {} succeeded OrderProcess rows (priorFailureLookbackDays={})",
        chunkLabel, processes.size(), PRIOR_LOOKBACK_DAYS)

    List procMaps = []
    List orderPks = []
    processes.each { r ->
        PK pk = r[0] as PK
        String defName = r[2] ?: ''
        String spanId = r[5] != null ? String.valueOf(r[5]) : ''
        String traceId = r[6] != null ? String.valueOf(r[6]) : ''
        PK orderPk = r[7] as PK
        String orderCode = r[8] ?: ''
        Date procCreated = r[3] as Date
        Date procModified = r[4] as Date
        Date orderCreated = r[9] as Date
        Date modSubmitted = r[10] as Date
        String agentAccount = r[11] != null ? String.valueOf(r[11]).trim() : ''
        String agentName = r[12] != null ? String.valueOf(r[12]).trim() : ''
        orderPks << orderPk
        procMaps << [
            pk               : pk,
            processCode      : r[1] ?: '',
            defName          : defName,
            state            : 'SUCCEEDED',
            spanId           : spanId,
            traceId          : traceId,
            procCreated      : procCreated,
            procModified     : procModified,
            orderPk          : orderPk,
            orderCode        : orderCode,
            orderCreated     : orderCreated,
            modSubmitted     : modSubmitted,
            procCreatedStr   : procCreated ? String.valueOf(procCreated) : '',
            procModifiedStr  : procModified ? String.valueOf(procModified) : '',
            orderCreatedStr  : orderCreated ? String.valueOf(orderCreated) : '',
            modSubmittedStr  : modSubmitted ? String.valueOf(modSubmitted) : '',
            sapOrderNumber   : orderCode.startsWith('BC-') ? orderCode.substring(3) : '',
            kind             : defName.contains('sap-order-process') ? 'CREATE' : 'MODIFY',
            agentAccount     : agentAccount,
            agentName        : agentName
        ]
    }

    Map enrichment = loadOrderEnrichment(orderPks, priorLookbackSince)

    StringBuilder csv = new StringBuilder()
    csv.append(HEADER.join(',')).append('\n')
    int written = 0
    procMaps.each { Map proc ->
        def tasks = loadTasks(proc.pk as PK)
        csv.append(buildRow(proc, tasks, enrichment)).append('\n')
        written++
    }

    byte[] csvBytes = csv.toString().getBytes(StandardCharsets.UTF_8)
    log.info("CSV built rows={} bytes={} file={}", written, csvBytes.length, chunkFileName)
    [
        rows     : written,
        fileName : chunkFileName,
        window   : chunkLabel,
        since    : wSince,
        until    : wUntil,
        csvBytes : csvBytes
    ]
}

def sendEmailFor = { Map built, String subjectExtra = '', boolean attachCsv = true ->
    if (SKIP_EMAIL || (MODE == 'backfill' && BACKFILL_SKIP_EMAIL)) {
        return 'skipped-by-config'
    }
    if (!EMAIL_TO) {
        log.warn('order.timings.report.email.to not set — skipping email')
        return 'skipped-no-to'
    }

    List recipients = new ArrayList<>(EMAIL_TO)
    EMAIL_CC.each { if (it && !recipients.contains(it)) recipients.add(it) }

    try {
        def configurationService = spring.getBean('configurationService')
        def bayerEmailService = spring.getBean('bayerEmailService')
        def env = configurationService.getConfiguration().getString('env', '')
        def from = configurationService.getConfiguration().getString('onedce.general.mailbox')
        if (!from) {
            from = EMAIL_FROM
        }

        String subject = subjectExtra ?: "Order step timings — ${built.fileName}"
        if (env && !'PRD'.equalsIgnoreCase(env)) {
            subject = '[NON-PROD] ' + subject
        }

        String body = """\
<div style="font-family:Arial,Helvetica,sans-serif;color:#111827;">
<div style="font-size:16px;font-weight:bold;margin-bottom:8px;">Order step timings report (cbus)</div>
<div style="font-size:13px;line-height:1.6;">
Store: <strong>${STORE}</strong><br/>
Mode: <strong>${MODE}</strong><br/>
Window: ${built.window}<br/>
Rows: <strong>${built.rows}</strong><br/>
File: ${built.fileName ?: '(none)'}<br/>
All durations are in ms (cartToOrderMs, ERP step api/wall, etc.).<br/>
Also includes lineItemCount and prior API failure columns
(failed BP + chain FAILURE, lookback ${PRIOR_LOOKBACK_DAYS}d).
</div>
</div>
""".stripIndent()

        Email email = new Email()
        email.setFromEmail(from)
        email.setSubject(subject)
        email.setEmailList(recipients)
        email.setBody(body)
        if (attachCsv && built.csvBytes != null && built.fileName) {
            EmailAttachment attachment = new EmailAttachment()
            attachment.setFileName(built.fileName as String)
            attachment.setEncoding('base64')
            attachment.setSource(Base64.getEncoder().encodeToString(built.csvBytes as byte[]))
            email.setAttachments([attachment])
        }

        bayerEmailService.sendEmail(email, 'cbus', UUID.randomUUID().toString())
        String msg = "SENT to ${recipients.join(', ')}"
        log.info('Email: {}', msg)
        return msg
    } catch (Exception e) {
        log.error('Email FAILED: {}', e.message, e)
        return "FAILED: ${e.message}"
    }
}

def uploadS3For = { Map built ->
    if (SKIP_S3) return 'skipped-by-config'
    if (!S3_BUCKET) {
        log.warn('order.timings.report.s3.bucket not set — skipping S3')
        return 'skipped-no-bucket'
    }
    if (!S3_REGION) {
        log.warn('order.timings.report.s3.region not set — skipping S3')
        return 'skipped-no-region'
    }
    if (!VAULT_AWS_PATH) {
        log.warn('onedce.aws.path not set — cannot load AWS credentials from Vault')
        return 'skipped-no-vault-path'
    }
    if (built.csvBytes == null || !built.fileName) return 'skipped-no-payload'

    String prefix = S3_PREFIX ?: ''
    if (prefix && !prefix.endsWith('/')) prefix = prefix + '/'
    String objectKey = prefix + built.fileName

    try {
        def intSvc = spring.getBean('bayerAmazonS3IntService')
        def m = intSvc.getClass().getDeclaredMethod('getOneDCEs3Client', Regions)
        m.accessible = true
        def s3 = m.invoke(intSvc, Regions.fromName(S3_REGION))

        byte[] csvBytes = built.csvBytes as byte[]
        def meta = new ObjectMetadata()
        meta.setContentLength(csvBytes.length as long)
        meta.setContentType('text/csv')
        s3.putObject(S3_BUCKET, objectKey, new ByteArrayInputStream(csvBytes), meta)

        String uri = "s3://${S3_BUCKET}/${objectKey}"
        log.info('Uploaded via Vault S3 client (vault={}): {}', VAULT_AWS_PATH, uri)
        return uri
    } catch (Throwable t) {
        log.error('S3 upload via Vault failed (path={}): {}', VAULT_AWS_PATH, t.message, t)
        return "error-vault-s3:${t.message}"
    }
}

// ---------------------------------------------------------------------------
// Resolve windows and run
// ---------------------------------------------------------------------------
Date now = new Date()
List windows = []

if (MODE == 'backfill') {
    Date rangeStart = parseConfigDate(BACKFILL_FROM_CFG, false) ?: marketYearStartUtc(now)
    Date rangeEnd = parseConfigDate(BACKFILL_TO_CFG, true) ?: now
    if (BACKFILL_RESUME_CFG) {
        Date resume = parseConfigDate(BACKFILL_RESUME_CFG, false)
        if (resume != null && resume.after(rangeStart)) {
            rangeStart = resume
        }
    }
    Date myEnd = marketYearEndExclusiveUtc(marketYearStartUtc(rangeStart))
    if (rangeEnd.after(myEnd)) {
        log.warn('Backfill to={} clipped to market-year end exclusive {}', isoUtc.format(rangeEnd), isoUtc.format(myEnd))
        rangeEnd = myEnd
    }
    if (rangeEnd.after(now)) rangeEnd = now
    if (!rangeStart.before(rangeEnd)) {
        throw new IllegalArgumentException("Backfill range empty: from=${isoUtc.format(rangeStart)} to=${isoUtc.format(rangeEnd)}")
    }

    int chunkHours = Math.max(1, BACKFILL_CHUNK_HOURS)
    Calendar cursor = Calendar.getInstance(UTC)
    cursor.setTime(rangeStart)
    int planned = 0
    while (cursor.getTime().before(rangeEnd)) {
        Date wSince = cursor.getTime()
        cursor.add(Calendar.HOUR_OF_DAY, chunkHours)
        Date wUntil = cursor.getTime().after(rangeEnd) ? rangeEnd : cursor.getTime()
        windows << [wSince, wUntil]
        planned++
        if (BACKFILL_MAX_CHUNKS > 0 && planned >= BACKFILL_MAX_CHUNKS) break
    }
    log.info(
        "Backfill mode store={} marketYearStart={} range={} → {} chunks(chunkHours={}, maxChunks={})",
        STORE, dayStamp.format(marketYearStartUtc(rangeStart)),
        "${isoUtc.format(rangeStart)} → ${isoUtc.format(rangeEnd)}",
        windows.size(), chunkHours, BACKFILL_MAX_CHUNKS
    )
} else {
    Calendar windowEnd = Calendar.getInstance()
    Calendar windowStart = Calendar.getInstance()
    windowStart.add(Calendar.HOUR_OF_DAY, -HOURS_BACK)
    windows << [windowStart.getTime(), windowEnd.getTime()]
    log.info("Hourly mode store={} window={}", STORE,
        "${isoUtc.format(windowStart.getTime())} → ${isoUtc.format(windowEnd.getTime())} (UTC)")
}

int totalRows = 0
int uploaded = 0
int skippedEmpty = 0
List s3Uris = []
List errors = []
String lastFile = ''
String emailStatus = 'skipped'
Date firstSince = windows ? windows[0][0] as Date : null
Date lastUntil = windows ? windows[-1][1] as Date : null

windows.eachWithIndex { pair, idx ->
    Date wSince = pair[0] as Date
    Date wUntil = pair[1] as Date
    try {
        Map built = buildCsvForWindow(wSince, wUntil)
        totalRows += (built.rows as int)
        lastFile = built.fileName as String

        boolean empty = (built.rows as int) == 0
        if (empty && !BACKFILL_UPLOAD_EMPTY && MODE == 'backfill') {
            skippedEmpty++
            log.info('Skipping empty backfill chunk {}/{} {}', idx + 1, windows.size(), built.window)
            return
        }

        String s3 = uploadS3For(built)
        if (s3?.startsWith('s3://')) {
            uploaded++
            s3Uris << s3
        } else if (s3?.startsWith('error')) {
            errors << "${built.fileName}:${s3}"
        }

        if (MODE != 'backfill') {
            emailStatus = sendEmailFor(
                built,
                "cbus order step timings — last ${HOURS_BACK}h — ${fileStamp.format(wUntil)} UTC",
                true
            )
        }
    } catch (Throwable t) {
        String msg = "chunk ${idx + 1}/${windows.size()} ${isoUtc.format(wSince)}: ${t.message}"
        errors << msg
        log.error('Window failed: {}', msg, t)
    }
}

if (MODE == 'backfill' && !BACKFILL_SKIP_EMAIL && !SKIP_EMAIL) {
    String nextResume = lastUntil ? dayStamp.format(lastUntil) : ''
    emailStatus = sendEmailFor(
        [
            rows    : totalRows,
            fileName: lastFile,
            window  : firstSince && lastUntil
                ? "${isoUtc.format(firstSince)} → ${isoUtc.format(lastUntil)} (UTC)"
                : '(none)',
            csvBytes: null
        ],
        "cbus order step timings BACKFILL — ${uploaded} files / ${totalRows} rows — next resumeFrom=${nextResume}",
        false
    )
}

String summary = "OK mode=${MODE} store=${STORE} chunks=${windows.size()} rows=${totalRows} uploaded=${uploaded} " +
    "skippedEmpty=${skippedEmpty} lastFile=${lastFile} email=${emailStatus} " +
    "errors=${errors.size()}" +
    (errors ? " detail=${errors.take(5).join(' | ')}" : '') +
    (MODE == 'backfill' && lastUntil ? " nextResumeFrom=${dayStamp.format(lastUntil)}" : '')
log.info(summary)
return summary

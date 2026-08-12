import std/[json, os, times]
import db_connector/db_sqlite
import ./[backend, jobkey, priority, schedule, types]

type SqliteBackend* = ref object of QueueBackend
  dbPath: string
  db: DbConn

proc newSqliteBackend*(): SqliteBackend =
  SqliteBackend()

proc requireDb(backend: SqliteBackend): DbConn =
  if backend.db == nil:
    raise backendNotConfigured()
  backend.db

proc execSql(db: DbConn; query: SqlQuery; args: varargs[string, `$`]) =
  db.exec(query, args)

proc applyPerformancePragmas(db: DbConn) =
  discard db.getValue(sql"PRAGMA journal_mode = WAL")
  db.execSql(sql"PRAGMA synchronous = NORMAL")
  db.execSql(sql"PRAGMA busy_timeout = 5000")
  db.execSql(sql"PRAGMA temp_store = MEMORY")

proc nowMillis(): int64 =
  int64(epochTime() * 1000.0)

proc newLeaseId(jobId: string; leasedUntil: int64): string =
  jobId & "|" & $leasedUntil

proc jobIdFromPayload(raw: string): string =
  let payload = parseJson(raw)
  if "id" in payload:
    payload["id"].getStr()
  else:
    ""

proc isBlockedTask(payload: JsonNode; blockedTasks: openArray[string]): bool =
  if blockedTasks.len == 0 or "taskName" notin payload:
    return false
  let taskName = payload["taskName"].getStr()
  for blocked in blockedTasks:
    if blocked == taskName:
      return true

method setup*(backend: SqliteBackend; basePath: string; queues: openArray[string]) {.gcsafe.} =
  {.cast(gcsafe).}:
    discard existsOrCreateDir(basePath)
    backend.dbPath = basePath / "quee.sqlite3"
    backend.db = open(backend.dbPath, "", "", "")
    backend.db.applyPerformancePragmas()
    backend.db.execSql(sql"""
      CREATE TABLE IF NOT EXISTS jobs (
        queue TEXT NOT NULL,
        storage_key TEXT NOT NULL,
        payload TEXT NOT NULL,
        state TEXT NOT NULL DEFAULT 'queued',
        leased_until INTEGER NOT NULL DEFAULT 0,
        lease_id TEXT NOT NULL DEFAULT '',
        attempts INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (queue, storage_key)
      )
    """)
    try:
      backend.db.execSql(sql"ALTER TABLE jobs ADD COLUMN state TEXT NOT NULL DEFAULT 'queued'")
    except DbError:
      discard
    try:
      backend.db.execSql(sql"ALTER TABLE jobs ADD COLUMN leased_until INTEGER NOT NULL DEFAULT 0")
    except DbError:
      discard
    try:
      backend.db.execSql(sql"ALTER TABLE jobs ADD COLUMN lease_id TEXT NOT NULL DEFAULT ''")
    except DbError:
      discard
    try:
      backend.db.execSql(sql"ALTER TABLE jobs ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0")
    except DbError:
      discard
    backend.db.execSql(sql"CREATE INDEX IF NOT EXISTS idx_jobs_queue_key ON jobs(queue, storage_key)")
    backend.db.execSql(sql"CREATE INDEX IF NOT EXISTS idx_jobs_queue_state_lease ON jobs(queue, state, leased_until)")

method close*(backend: SqliteBackend) {.gcsafe.} =
  {.cast(gcsafe).}:
    if backend.db != nil:
      backend.db.close()
      backend.db = nil

method storagePath*(backend: SqliteBackend; queue: string): string {.gcsafe.} =
  {.cast(gcsafe).}:
    backend.dbPath

method enqueue*(backend: SqliteBackend; queue: string; payload: JsonNode) {.gcsafe.} =
  {.cast(gcsafe).}:
    let db = backend.requireDb()
    db.execSql(
      sql"INSERT OR REPLACE INTO jobs(queue, storage_key, payload) VALUES (?, ?, ?)",
      queue,
      jobStorageKey(payload),
      $payload,
    )

method claimDue*(
  backend: SqliteBackend; queue: string; blockedTasks: openArray[string]; leaseTimeoutMs: int
): ClaimedJob {.gcsafe.} =
  {.cast(gcsafe).}:
    let db = backend.requireDb()
    let nowMs = nowMillis()
    let leasedUntil = nowMs + leaseTimeoutMs.int64
    db.execSql(sql"BEGIN IMMEDIATE")
    try:
      var rawJob = ""
      var storageKey = ""
      var bestPri = MaxPriority + 1

      for row in db.fastRows(
        sql"""
          SELECT storage_key, payload FROM jobs
          WHERE queue = ? AND (state = 'queued' OR (state = 'running' AND leased_until <= ?))
          ORDER BY storage_key
        """,
        queue,
        $nowMs,
      ):
        let key = row[0]
        let val = row[1]
        if isJobStorageKey(key):
          if isJobDue(val):
            let payload = parseJson(val)
            if payload.isBlockedTask(blockedTasks):
              continue
            let pri = jobPriority(payload)
            if rawJob.len == 0 or pri < bestPri:
              rawJob = val
              storageKey = key
              bestPri = pri
            break
          continue

        if not isJobDue(val):
          continue
        let payload = parseJson(val)
        if payload.isBlockedTask(blockedTasks):
          continue
        let pri = jobPriority(payload)
        if rawJob.len == 0 or pri < bestPri:
          rawJob = val
          storageKey = key
          bestPri = pri

      if rawJob.len > 0:
        let leaseId = newLeaseId(jobIdFromPayload(rawJob), leasedUntil)
        db.execSql(
          sql"""
            UPDATE jobs
            SET state = 'running', leased_until = ?, lease_id = ?, attempts = attempts + 1
            WHERE queue = ? AND storage_key = ?
          """,
          $leasedUntil,
          leaseId,
          queue,
          storageKey,
        )

      db.execSql(sql"COMMIT")
      if rawJob.len > 0:
        result.payload = parseJson(rawJob)
        result.id = result.payload["id"].getStr()
        result.leaseId = newLeaseId(result.id, leasedUntil)
    except CatchableError:
      db.execSql(sql"ROLLBACK")
      raise

method requeue*(backend: SqliteBackend; queue: string; payload: JsonNode) {.gcsafe.} =
  {.cast(gcsafe).}:
    let db = backend.requireDb()
    db.execSql(
      sql"""
        INSERT OR REPLACE INTO jobs(queue, storage_key, payload, state, leased_until, lease_id, attempts)
        VALUES (?, ?, ?, 'queued', 0, '', 0)
      """,
      queue,
      jobStorageKey(payload),
      $payload,
    )

method complete*(
  backend: SqliteBackend; queue: string; jobId: string; leaseId: string; nextPayload: JsonNode = nil
) {.gcsafe.} =
  {.cast(gcsafe).}:
    let db = backend.requireDb()
    db.execSql(sql"BEGIN IMMEDIATE")
    try:
      var storageKey = ""
      for row in db.fastRows(
        sql"SELECT storage_key, payload, lease_id FROM jobs WHERE queue = ? AND state = 'running'",
        queue,
      ):
        if row[2] == leaseId and jobIdFromPayload(row[1]) == jobId:
          storageKey = row[0]
          break

      if storageKey.len > 0:
        db.execSql(sql"DELETE FROM jobs WHERE queue = ? AND storage_key = ?", queue, storageKey)
      if storageKey.len > 0 and nextPayload != nil:
        db.execSql(
          sql"""
            INSERT OR REPLACE INTO jobs(queue, storage_key, payload, state, leased_until, lease_id, attempts)
            VALUES (?, ?, ?, 'queued', 0, '', 0)
          """,
          queue,
          jobStorageKey(nextPayload),
          $nextPayload,
        )
      db.execSql(sql"COMMIT")
    except CatchableError:
      db.execSql(sql"ROLLBACK")
      raise

method release*(backend: SqliteBackend; queue: string; jobId: string; leaseId: string) {.gcsafe.} =
  {.cast(gcsafe).}:
    let db = backend.requireDb()
    db.execSql(sql"BEGIN IMMEDIATE")
    try:
      var storageKey = ""
      for row in db.fastRows(
        sql"SELECT storage_key, payload, lease_id FROM jobs WHERE queue = ? AND state = 'running'",
        queue,
      ):
        if row[2] == leaseId and jobIdFromPayload(row[1]) == jobId:
          storageKey = row[0]
          break
      if storageKey.len > 0:
        db.execSql(
          sql"UPDATE jobs SET state = 'queued', leased_until = 0, lease_id = '' WHERE queue = ? AND storage_key = ?",
          queue,
          storageKey,
        )
      db.execSql(sql"COMMIT")
    except CatchableError:
      db.execSql(sql"ROLLBACK")
      raise

method cancel*(backend: SqliteBackend; queue: string; jobId: string): bool {.gcsafe.} =
  {.cast(gcsafe).}:
    let db = backend.requireDb()
    var storageKey = ""
    for row in db.fastRows(
      sql"SELECT storage_key, payload FROM jobs WHERE queue = ? AND state = 'queued' ORDER BY storage_key",
      queue,
    ):
      if row[0] == jobId:
        storageKey = row[0]
        break
      let payload = parseJson(row[1])
      if "id" in payload and payload["id"].getStr() == jobId:
        storageKey = row[0]
        break

    if storageKey.len > 0:
      db.execSql(sql"DELETE FROM jobs WHERE queue = ? AND storage_key = ?", queue, storageKey)
      result = true

method discardMissed*(backend: SqliteBackend; queue: string): int {.gcsafe.} =
  {.cast(gcsafe).}:
    let db = backend.requireDb()
    var deletes: seq[string] = @[]
    var updates: seq[JsonNode] = @[]

    db.execSql(sql"BEGIN IMMEDIATE")
    try:
      for row in db.fastRows(
        sql"SELECT storage_key, payload FROM jobs WHERE queue = ? AND state = 'queued' ORDER BY storage_key",
        queue,
      ):
        let key = row[0]
        let val = row[1]
        if not isJobDue(val):
          continue

        let payload = parseJson(val)
        let sched =
          if "schedule" in payload: scheduleFromJson(payload["schedule"])
          else: JobSchedule(kind: skOnce)

        deletes.add(key)
        if sched.kind != skOnce:
          var next = payload
          next["runAt"] = %computeNextRunAt(sched)
          updates.add(next)

      for key in deletes:
        db.execSql(sql"DELETE FROM jobs WHERE queue = ? AND storage_key = ?", queue, key)
      for payload in updates:
        db.execSql(
          sql"""
            INSERT OR REPLACE INTO jobs(queue, storage_key, payload, state, leased_until, lease_id, attempts)
            VALUES (?, ?, ?, 'queued', 0, '', 0)
          """,
          queue,
          jobStorageKey(payload),
          $payload,
        )
      db.execSql(sql"COMMIT")
    except CatchableError:
      db.execSql(sql"ROLLBACK")
      raise

    deletes.len

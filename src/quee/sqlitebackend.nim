import std/[json, os]
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

method setup*(backend: SqliteBackend; basePath: string; queues: openArray[string]) {.gcsafe.} =
  {.cast(gcsafe).}:
    discard existsOrCreateDir(basePath)
    backend.dbPath = basePath / "quee.sqlite3"
    backend.db = open(backend.dbPath, "", "", "")
    backend.db.execSql(sql"""
      CREATE TABLE IF NOT EXISTS jobs (
        queue TEXT NOT NULL,
        storage_key TEXT NOT NULL,
        payload TEXT NOT NULL,
        PRIMARY KEY (queue, storage_key)
      )
    """)
    backend.db.execSql(sql"CREATE INDEX IF NOT EXISTS idx_jobs_queue_key ON jobs(queue, storage_key)")

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

method claimDue*(backend: SqliteBackend; queue: string): ClaimedJob {.gcsafe.} =
  {.cast(gcsafe).}:
    let db = backend.requireDb()
    db.execSql(sql"BEGIN IMMEDIATE")
    try:
      var rawJob = ""
      var storageKey = ""
      var bestPri = MaxPriority + 1

      for row in db.fastRows(sql"SELECT storage_key, payload FROM jobs WHERE queue = ? ORDER BY storage_key", queue):
        let key = row[0]
        let val = row[1]
        if isJobStorageKey(key):
          if isJobDue(val):
            let pri = jobPriority(parseJson(val))
            if rawJob.len == 0 or pri < bestPri:
              rawJob = val
              storageKey = key
              bestPri = pri
            break
          continue

        if not isJobDue(val):
          continue
        let pri = jobPriority(parseJson(val))
        if rawJob.len == 0 or pri < bestPri:
          rawJob = val
          storageKey = key
          bestPri = pri

      if rawJob.len > 0:
        db.execSql(sql"DELETE FROM jobs WHERE queue = ? AND storage_key = ?", queue, storageKey)

      db.execSql(sql"COMMIT")
      if rawJob.len > 0:
        result.payload = parseJson(rawJob)
        result.id = result.payload["id"].getStr()
    except CatchableError:
      db.execSql(sql"ROLLBACK")
      raise

method requeue*(backend: SqliteBackend; queue: string; payload: JsonNode) {.gcsafe.} =
  {.cast(gcsafe).}:
    let db = backend.requireDb()
    db.execSql(
      sql"INSERT OR REPLACE INTO jobs(queue, storage_key, payload) VALUES (?, ?, ?)",
      queue,
      jobStorageKey(payload),
      $payload,
    )

method cancel*(backend: SqliteBackend; queue: string; jobId: string): bool {.gcsafe.} =
  {.cast(gcsafe).}:
    let db = backend.requireDb()
    var storageKey = ""
    for row in db.fastRows(sql"SELECT storage_key, payload FROM jobs WHERE queue = ? ORDER BY storage_key", queue):
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
      for row in db.fastRows(sql"SELECT storage_key, payload FROM jobs WHERE queue = ? ORDER BY storage_key", queue):
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
          sql"INSERT OR REPLACE INTO jobs(queue, storage_key, payload) VALUES (?, ?, ?)",
          queue,
          jobStorageKey(payload),
          $payload,
        )
      db.execSql(sql"COMMIT")
    except CatchableError:
      db.execSql(sql"ROLLBACK")
      raise

    deletes.len

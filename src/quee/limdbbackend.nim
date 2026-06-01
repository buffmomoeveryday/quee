import std/[json, os, tables]
import limdb
import ./[backend, jobkey, priority, schedule, types]

type LimdbBackend* = ref object of QueueBackend
  basePath: string
  queuePaths: Table[string, string]
  queueDatabases: Table[string, Database[string, string]]

proc newLimdbBackend*(): LimdbBackend =
  LimdbBackend(
    queuePaths: initTable[string, string](),
    queueDatabases: initTable[string, Database[string, string]](),
  )

proc openDb(backend: LimdbBackend; queue: string): Database[string, string] =
  if queue notin backend.queuePaths:
    raise newException(ValueError, "Unknown queue: '" & queue & "'")
  let path = backend.queuePaths[queue]
  if path notin backend.queueDatabases:
    backend.queueDatabases[path] = initDatabase(path)
  backend.queueDatabases[path]

proc isBlockedTask(payload: JsonNode; blockedTasks: openArray[string]): bool =
  if blockedTasks.len == 0 or "taskName" notin payload:
    return false
  let taskName = payload["taskName"].getStr()
  for blocked in blockedTasks:
    if blocked == taskName:
      return true

method setup*(backend: LimdbBackend; basePath: string; queues: openArray[string]) {.gcsafe.} =
  {.cast(gcsafe).}:
    backend.basePath = basePath
    backend.queuePaths.clear()
    discard existsOrCreateDir(basePath)
    for queue in queues:
      let qpath = basePath / queue
      createDir qpath
      backend.queuePaths[queue] = qpath

method close*(backend: LimdbBackend) {.gcsafe.} =
  {.cast(gcsafe).}:
    for _, db in backend.queueDatabases:
      db.close()
    backend.queueDatabases.clear()

method storagePath*(backend: LimdbBackend; queue: string): string {.gcsafe.} =
  {.cast(gcsafe).}:
    if queue notin backend.queuePaths:
      raise newException(ValueError, "Unknown queue: '" & queue & "'")
    backend.queuePaths[queue]

method enqueue*(backend: LimdbBackend; queue: string; payload: JsonNode) {.gcsafe.} =
  {.cast(gcsafe).}:
    let db = backend.openDb(queue)
    db.withTransaction t:
      t[jobStorageKey(payload)] = $payload

method claimDue*(
  backend: LimdbBackend; queue: string; blockedTasks: openArray[string]
): ClaimedJob {.gcsafe.} =
  {.cast(gcsafe).}:
    let db = backend.openDb(queue)
    var rawJob = ""
    var storageKey = ""
    db.withTransaction t:
      var bestPri = MaxPriority + 1
      for key, val in t.pairs:
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
        t.del(storageKey)

    if rawJob.len > 0:
      result.payload = parseJson(rawJob)
      result.id = result.payload["id"].getStr()

method requeue*(backend: LimdbBackend; queue: string; payload: JsonNode) {.gcsafe.} =
  {.cast(gcsafe).}:
    let db = backend.openDb(queue)
    db.withTransaction t:
      t[jobStorageKey(payload)] = $payload

method cancel*(backend: LimdbBackend; queue: string; jobId: string): bool {.gcsafe.} =
  {.cast(gcsafe).}:
    let db = backend.openDb(queue)
    var keyToDelete = ""
    db.withTransaction t:
      for key, val in t.pairs:
        if key == jobId:
          keyToDelete = key
          break

        let payload = parseJson(val)
        if "id" in payload and payload["id"].getStr() == jobId:
          keyToDelete = key
          break

      if keyToDelete.len > 0:
        t.del(keyToDelete)
        result = true

method discardMissed*(backend: LimdbBackend; queue: string): int {.gcsafe.} =
  {.cast(gcsafe).}:
    let db = backend.openDb(queue)
    var deletes: seq[string] = @[]
    var updates: seq[JsonNode] = @[]

    db.withTransaction t:
      for key, val in t.pairs:
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
        t.del(key)
      for payload in updates:
        t[jobStorageKey(payload)] = $payload

    deletes.len

import std/[json, os, strutils, tables, times]
import limdb
import ./[types, registry, schedule, log, priority, jobkey]

var workerThreads: seq[Thread[void]]
var workersRunning = false

proc requeueRecurring(db: Database, payload: JsonNode) =
  let sched = scheduleFromJson(payload["schedule"])
  if sched.kind == skOnce:
    return
  var next = payload
  next["runAt"] = %computeNextRunAt(sched)
  db.withTransaction t:
    t[jobStorageKey(next)] = $next

proc processOneInQueue(path: string): bool =
  var rawJob = ""
  var storageKey = ""
  var db: Database[string, string]

  withQueeDbLock:
    db = openQueueDb(path)
    db.withTransaction t:
      var bestPri = MaxPriority + 1
      for key, val in t.pairs:
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
        t.del(storageKey)

  if rawJob.len == 0:
    return false

  let payload = parseJson(rawJob)
  let taskName = payload["taskName"].getStr()
  let sched =
    if "schedule" in payload: scheduleFromJson(payload["schedule"])
    else: JobSchedule(kind: skOnce)

  {.cast(gcsafe).}:
    if hasKey(queeRegistry.handlers, taskName):
      queeRegistry.handlers[taskName](payload["args"])
    else:
      queeWarn("No handler registered for: " & taskName)

  if sched.kind != skOnce:
    withQueeDbLock:
      requeueRecurring(db, payload)

  true

proc processOne*(): bool {.gcsafe.} =
  var paths: seq[string] = @[]

  {.cast(gcsafe).}:
    for _, path in queeRegistry.queuePaths:
      paths.add(path)

  for path in paths:
    if processOneInQueue(path):
      return true

  false

proc workerLoop() {.gcsafe.} =
  while true:
    if not processOne():
      var pollMs = DefaultPollIntervalMs
      {.cast(gcsafe).}:
        pollMs = queeRegistry.pollIntervalMs
      sleep(pollMs)

proc workersAreRunning*(): bool =
  workersRunning

proc startQuee*(pollIntervalMs: int = 0; concurrency: int = 0) =
  ## Start background worker thread(s). Prints registered tasks first.
  ##
  ## - ``pollIntervalMs > 0``: update poll interval before starting.
  ## - ``concurrency > 0``: number of parallel worker threads (like Celery ``-c``); else uses
  ##   ``initQuee`` / ``setWorkerConcurrency`` (default 1).
  if workersRunning:
    raise newException(ValueError, "Quee workers are already running (call waitForQuee first)")

  initQueeDbLock()

  if pollIntervalMs > 0:
    setPollInterval(pollIntervalMs)
  if concurrency > 0:
    setWorkerConcurrency(concurrency)

  let n = workerConcurrency()
  var names: seq[string] = @[]
  var pollMs = DefaultPollIntervalMs
  {.cast(gcsafe).}:
    names = queeRegistry.queues
    pollMs = queeRegistry.pollIntervalMs

  printRegisteredTasks()
  queeInfo(
    "Background worker started (concurrency " & $n & ", poll " & $pollMs &
      "ms, queues: " & names.join(", ") & ")",
  )

  workerThreads.setLen(n)
  for i in 0 ..< n:
    createThread(workerThreads[i], workerLoop)
  workersRunning = true

proc waitForQuee*() =
  ## Block until all worker threads exit (runs forever in normal use).
  if not workersRunning:
    return
  for i in 0 ..< workerThreads.len:
    joinThread(workerThreads[i])
  workerThreads.setLen(0)
  workersRunning = false

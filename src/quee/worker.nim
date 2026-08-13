import std/[json, os, strutils, tables]
import ./[types, backend, registry, schedule, log, dashboard]

var workerThreads: seq[Thread[void]]
var workersRunning = false

proc nextRecurringPayload(payload: JsonNode): JsonNode =
  let sched = scheduleFromJson(payload["schedule"])
  if sched.kind == skOnce:
    return payload
  result = payload
  result["runAt"] = %computeNextRunAt(sched)

proc processOneInQueue(queue: string): bool =
  var payload: JsonNode
  var runningJobId = ""
  var runningLeaseId = ""
  var runningTaskName = ""

  withQueeDbLock:
    let claimed = currentBackend().claimDue(queue, blockedTaskNames(), jobLeaseTimeout())
    if claimed.id.len > 0:
      payload = claimed.payload
      runningJobId = claimed.id
      runningLeaseId = claimed.leaseId
      runningTaskName = payload["taskName"].getStr()
      markJobRunning(runningJobId, runningTaskName, queue, runningLeaseId)

  if runningJobId.len == 0:
    return false

  let sched =
    if "schedule" in payload: scheduleFromJson(payload["schedule"])
    else: JobSchedule(kind: skOnce)

  try:
    {.cast(gcsafe).}:
      if hasKey(queeRegistry.handlers, runningTaskName):
        queeRegistry.handlers[runningTaskName](payload["args"])
      else:
        queeWarn("No handler registered for: " & runningTaskName)

    let nextPayload =
      if sched.kind != skOnce and not jobCancellationRequested(runningJobId):
        nextRecurringPayload(payload)
      else:
        nil

    withQueeDbLock:
      currentBackend().complete(queue, runningJobId, runningLeaseId, nextPayload)
  except CatchableError as e:
    let msg = e.msg
    queeWarn(
      "Job failed: " & runningTaskName & " (" & runningJobId & "): " & msg,
    )
    withQueeDbLock:
      currentBackend().fail(
        queue,
        runningJobId,
        runningLeaseId,
        maxAttempts(),
        retryDelay(),
        retryBackoff(),
        msg,
      )
  finally:
    if runningJobId.len > 0:
      unmarkJobRunning(runningJobId, runningTaskName)
      clearJobCancellation(runningJobId)

  true

proc processOne*(): bool {.gcsafe.} =
  var queues: seq[string] = @[]

  {.cast(gcsafe).}:
    queues = queeRegistry.queues

  for queue in queues:
    if not queueIsPaused(queue) and processOneInQueue(queue):
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
  startMonitoringDashboard()
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

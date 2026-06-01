import std/[json, locks, os, random, tables, terminal]
import limdb
import ./[types, log, priority, schedule, jobkey]

var queueDatabases = initTable[string, Database[string, string]]()

const
  DefaultPollIntervalMs* = 200
  DefaultWorkerConcurrency* = 1
  MaxWorkerConcurrency* = 64

var queeDbLock: Lock
var queeDbLockInitialized = false

proc initQueeDbLock*() =
  if not queeDbLockInitialized:
    initLock(queeDbLock)
    queeDbLockInitialized = true

template withQueeDbLock*(body: untyped) =
  acquire(queeDbLock)
  try:
    body
  finally:
    release(queeDbLock)

var queeRegistry* {.global.}: QueeRegistry = QueeRegistry(
  handlers: initTable[string, TaskHandler](),
  tasks: initTable[string, TaskInfo](),
  taskOrder: @[],
  queues: @[],
  queuePaths: initTable[string, string](),
  defaultQueue: "default",
  pollIntervalMs: DefaultPollIntervalMs,
  workerConcurrency: DefaultWorkerConcurrency,
)

proc validateWorkerConcurrency*(n: int) =
  if n < 1 or n > MaxWorkerConcurrency:
    raise newException(
      ValueError,
      "worker concurrency must be 1.." & $MaxWorkerConcurrency & ", got " & $n,
    )

proc setWorkerConcurrency*(n: int) =
  ## Number of worker threads for the next ``startQuee``. Call before ``startQuee``.
  validateWorkerConcurrency(n)
  queeRegistry.workerConcurrency = n

proc workerConcurrency*(): int =
  queeRegistry.workerConcurrency

proc validatePollInterval*(ms: int) =
  if ms < 1:
    raise newException(ValueError, "poll interval must be at least 1 ms, got " & $ms)

proc setPollInterval*(ms: int) =
  ## Set how long the worker sleeps when no job is due (milliseconds). Safe before or after ``startQuee``.
  validatePollInterval(ms)
  queeRegistry.pollIntervalMs = ms

proc pollInterval*(): int =
  ## Current worker poll interval in milliseconds.
  queeRegistry.pollIntervalMs

proc closeQueueDatabases*() =
  ## Close all cached LMDB connections. Called on ``initQuee`` / ``resetQueeRegistry``.
  for _, db in queueDatabases:
    db.close()
  queueDatabases.clear()

proc openQueueDb*(path: string): Database[string, string] {.gcsafe.} =
  ## Reuse one limdb environment per queue directory path.
  {.cast(gcsafe).}:
    if path notin queueDatabases:
      queueDatabases[path] = initDatabase(path)
    queueDatabases[path]

proc discardMissedJobsInQueue(path: string): int =
  let db = openQueueDb(path)
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

proc discardMissedJobs*(): int =
  ## Drop jobs that are already due without running them.
  ##
  ## One-shot jobs are deleted. Recurring jobs are advanced to their next run
  ## time so app restarts/deploys do not catch up missed executions.
  var paths: seq[string] = @[]
  for _, path in queeRegistry.queuePaths:
    paths.add(path)

  withQueeDbLock:
    for path in paths:
      result += discardMissedJobsInQueue(path)

proc dbPath*(queue: string = ""): string =
  let name =
    if queue.len > 0: queue
    else: queeRegistry.defaultQueue
  if name notin queeRegistry.queuePaths:
    raise newException(ValueError, "Unknown queue: '" & name & "'")
  queeRegistry.queuePaths[name]

proc resolveQueue*(queue: string; taskDefault: string): string =
  ## Pick runtime ``queue=``, else the task's default queue, else `"default"``.
  if queue.len > 0:
    queue
  elif taskDefault.len > 0:
    taskDefault
  else:
    queeRegistry.defaultQueue

proc initQuee*(
  path: string;
  queues: openArray[string] = ["default"];
  pollIntervalMs: int = DefaultPollIntervalMs;
  workerConcurrency: int = DefaultWorkerConcurrency,
  skipMissedJobs: bool = false,
) =
  ## Open or create the job store. Each queue gets its own subdirectory under `path`.
  ##
  ## When ``skipMissedJobs`` is true, jobs already due at startup are skipped:
  ## one-shot jobs are deleted and recurring jobs are advanced to their next run.
  validatePollInterval(pollIntervalMs)
  validateWorkerConcurrency(workerConcurrency)
  closeQueueDatabases()
  discard existsOrCreateDir(path)
  queeRegistry.basePath = path
  queeRegistry.queues = @queues
  queeRegistry.queuePaths.clear()
  queeRegistry.pollIntervalMs = pollIntervalMs
  queeRegistry.workerConcurrency = workerConcurrency

  if "default" notin queues:
    raise newException(ValueError, "queues must include \"default\"")

  queeRegistry.defaultQueue = "default"

  for name in queues:
    let qpath = path / name
    createDir qpath
    queeRegistry.queuePaths[name] = qpath

  randomize()
  initQueeDbLock()

  if skipMissedJobs:
    discard discardMissedJobs()

proc registerHandler*(name: string, handler: TaskHandler) =
  ## Register a task handler (usually done by the `task` macro via `registerTask`).
  queeRegistry.handlers[name] = handler

proc registerTask*(
  name: string; defaultQueue: string; defaultPriority: int; handler: TaskHandler
) =
  ## Register a named task with default queue and priority (1 = highest, 10 = lowest).
  validatePriority(defaultPriority)
  queeRegistry.handlers[name] = handler
  if name notin queeRegistry.taskOrder:
    queeRegistry.taskOrder.add(name)
  queeRegistry.tasks[name] = TaskInfo(
    name: name,
    defaultQueue: defaultQueue,
    defaultPriority: defaultPriority,
  )

proc listTasks*(): seq[TaskInfo] =
  ## All tasks registered via the `task` macro, in definition order.
  for name in queeRegistry.taskOrder:
    if name in queeRegistry.tasks:
      result.add(queeRegistry.tasks[name])

proc printRegisteredTasks*() =
  ## Print every task the library knows about (call before or after `startQuee`).
  let tasks = listTasks()
  if tasks.len == 0:
    queeWarn("No tasks registered yet")
    return

  queeInfo("Registered tasks (" & $tasks.len & "):")
  for info in tasks:
    styledEcho(
      fgCyan, "  • ", resetStyle,
      fgGreen, info.name, resetStyle,
      fgCyan, "  → queue: ", resetStyle,
      fgYellow, info.defaultQueue, resetStyle,
      fgCyan, "  priority: ", resetStyle,
      fgYellow, $info.defaultPriority, resetStyle,
      "\n",
    )

proc resetQueeRegistry*() =
  ## Clear registered handlers. Intended for tests.
  closeQueueDatabases()
  queeRegistry.handlers.clear()
  queeRegistry.tasks.clear()
  queeRegistry.taskOrder = @[]
  queeRegistry.queuePaths.clear()
  queeRegistry.queues = @[]
  queeRegistry.basePath = ""
  queeRegistry.pollIntervalMs = DefaultPollIntervalMs
  queeRegistry.workerConcurrency = DefaultWorkerConcurrency

import std/[locks, random, tables, terminal]
import ./[types, log, priority, backend, limdbbackend, sqlitebackend]

var activeBackend: QueueBackend

const
  DefaultPollIntervalMs* = 200
  DefaultWorkerConcurrency* = 1
  MaxWorkerConcurrency* = 64

var queeDbLock: Lock
var queeDbLockInitialized = false
var cancellationLock: Lock
var cancellationLockInitialized = false
var cancelledJobs = initTable[string, bool]()
var runningJobs = initTable[string, bool]()
var currentJobId {.threadvar.}: string

proc initQueeDbLock*() =
  if not queeDbLockInitialized:
    initLock(queeDbLock)
    queeDbLockInitialized = true

proc initCancellationLock() =
  if not cancellationLockInitialized:
    initLock(cancellationLock)
    cancellationLockInitialized = true

template withQueeDbLock*(body: untyped) =
  acquire(queeDbLock)
  try:
    body
  finally:
    release(queeDbLock)

template withCancellationLock(body: untyped) =
  initCancellationLock()
  acquire(cancellationLock)
  try:
    body
  finally:
    release(cancellationLock)

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

proc newBackend*(kind: BackendKind): QueueBackend =
  case kind
  of bkLimdb:
    newLimdbBackend()
  of bkSqlite:
    newSqliteBackend()

proc currentBackend*(): QueueBackend {.gcsafe.} =
  {.cast(gcsafe).}:
    if activeBackend == nil:
      raise backendNotConfigured()
    activeBackend

proc closeQueueDatabases*() =
  ## Compatibility alias: close the currently configured backend.
  if activeBackend != nil:
    activeBackend.close()

proc markJobRunning*(jobId: string) {.gcsafe.} =
  {.cast(gcsafe).}:
    currentJobId = jobId
    withCancellationLock:
      runningJobs[jobId] = true

proc unmarkJobRunning*(jobId: string) {.gcsafe.} =
  {.cast(gcsafe).}:
    currentJobId = ""
    withCancellationLock:
      runningJobs.del(jobId)

proc currentJob*(): string {.gcsafe.} =
  currentJobId

proc requestJobCancellation(jobId: string) =
  withCancellationLock:
    cancelledJobs[jobId] = true

proc clearJobCancellation*(jobId: string) {.gcsafe.} =
  {.cast(gcsafe).}:
    withCancellationLock:
      cancelledJobs.del(jobId)

proc jobCancellationRequested*(jobId: string): bool {.gcsafe.} =
  {.cast(gcsafe).}:
    withCancellationLock:
      result = jobId in cancelledJobs

proc cancellationRequested*(): bool {.gcsafe.} =
  let jobId = currentJobId
  jobId.len > 0 and jobCancellationRequested(jobId)

proc jobIsRunning(jobId: string): bool =
  withCancellationLock:
    result = jobId in runningJobs

proc cancelJob*(jobId: string; queue: string = ""): bool =
  ## Cancel a queued/scheduled job by id.
  ##
  ## If the job has not started yet, it is removed from the backend and will not run.
  ## If it is already running, cancellation is cooperative: the handler should
  ## check ``cancellationRequested()`` and return early.
  if jobId.len == 0:
    return false

  var queues: seq[string] = @[]
  if queue.len > 0:
    if queue notin queeRegistry.queuePaths:
      raise newException(ValueError, "Unknown queue: '" & queue & "'")
    queues.add(queue)
  else:
    queues = queeRegistry.queues

  var removed = false
  withQueeDbLock:
    let backend = currentBackend()
    for queueName in queues:
      if backend.cancel(queueName, jobId):
        removed = true
        break

  if removed:
    clearJobCancellation(jobId)
    return true

  requestJobCancellation(jobId)
  if jobIsRunning(jobId):
    return true

  clearJobCancellation(jobId)
  false

proc discardMissedJobs*(): int =
  ## Drop jobs that are already due without running them.
  ##
  ## One-shot jobs are deleted. Recurring jobs are advanced to their next run
  ## time so app restarts/deploys do not catch up missed executions.
  withQueeDbLock:
    let backend = currentBackend()
    for queue in queeRegistry.queues:
      result += backend.discardMissed(queue)

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
  backendKind: BackendKind = bkLimdb,
  backend: QueueBackend = nil,
) =
  ## Open or create the job store using the selected or provided backend.
  ##
  ## When ``skipMissedJobs`` is true, jobs already due at startup are skipped:
  ## one-shot jobs are deleted and recurring jobs are advanced to their next run.
  validatePollInterval(pollIntervalMs)
  validateWorkerConcurrency(workerConcurrency)
  if "default" notin queues:
    raise newException(ValueError, "queues must include \"default\"")

  if activeBackend != nil:
    activeBackend.close()
  activeBackend =
    if backend == nil: newBackend(backendKind)
    else: backend
  activeBackend.setup(path, queues)

  queeRegistry.basePath = path
  queeRegistry.queues = @queues
  queeRegistry.queuePaths.clear()
  queeRegistry.pollIntervalMs = pollIntervalMs
  queeRegistry.workerConcurrency = workerConcurrency

  queeRegistry.defaultQueue = "default"

  for name in queues:
    queeRegistry.queuePaths[name] = activeBackend.storagePath(name)

  randomize()
  initQueeDbLock()
  initCancellationLock()

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
  withCancellationLock:
    cancelledJobs.clear()
    runningJobs.clear()

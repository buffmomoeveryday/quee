import std/[locks, random, tables, terminal]
import ./[types, log, priority, backend, memorybackend, sqlitebackend]

var activeBackend: QueueBackend

const
  DefaultPollIntervalMs* = 200
  DefaultWorkerConcurrency* = 1
  MaxWorkerConcurrency* = 64
  DefaultJobLeaseTimeoutMs* = 30_000
  DefaultMaxAttempts* = 3
  DefaultRetryDelayMs* = 1_000
  DefaultRetryBackoff* = 2.0

var queeDbLock: Lock
var queeDbLockInitialized = false
var cancellationLock: Lock
var cancellationLockInitialized = false
var cancelledJobs = initTable[string, bool]()
var runningJobs = initTable[string, bool]()
var runningTaskCounts = initTable[string, int]()
var currentJobId {.threadvar.}: string
var currentQueue {.threadvar.}: string
var currentLeaseId {.threadvar.}: string

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
    jobLeaseTimeoutMs: DefaultJobLeaseTimeoutMs,
    maxAttempts: DefaultMaxAttempts,
    retryDelayMs: DefaultRetryDelayMs,
    retryBackoff: DefaultRetryBackoff,
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

const UnlimitedTaskConcurrency* = 0

proc validateTaskConcurrency*(n: int) =
  if n < UnlimitedTaskConcurrency or n > MaxWorkerConcurrency:
    raise newException(
      ValueError,
      "task concurrency must be 0.." & $MaxWorkerConcurrency &
        " (0 means unlimited), got " & $n,
    )

proc validatePollInterval*(ms: int) =
  if ms < 1:
    raise newException(ValueError, "poll interval must be at least 1 ms, got " & $ms)

proc validateJobLeaseTimeout*(ms: int) =
  if ms < 1:
    raise newException(ValueError, "job lease timeout must be at least 1 ms, got " & $ms)

proc validateMaxAttempts*(n: int) =
  if n < 1:
    raise newException(ValueError, "max attempts must be at least 1, got " & $n)

proc validateRetryDelay*(ms: int) =
  if ms < 0:
    raise newException(ValueError, "retry delay must be >= 0 ms, got " & $ms)

proc validateRetryBackoff*(factor: float) =
  if factor < 1.0:
    raise newException(ValueError, "retry backoff must be >= 1.0, got " & $factor)

proc setPollInterval*(ms: int) =
  ## Set how long the worker sleeps when no job is due (milliseconds). Safe before or after ``startQuee``.
  validatePollInterval(ms)
  queeRegistry.pollIntervalMs = ms

proc pollInterval*(): int =
  ## Current worker poll interval in milliseconds.
  queeRegistry.pollIntervalMs

proc setJobLeaseTimeout*(ms: int) =
  ## Set how long a claimed job is reserved for a worker before another worker
  ## may retry it after a crash or hung handler.
  validateJobLeaseTimeout(ms)
  queeRegistry.jobLeaseTimeoutMs = ms

proc jobLeaseTimeout*(): int {.gcsafe.} =
  ## Current job lease timeout in milliseconds.
  {.cast(gcsafe).}:
    queeRegistry.jobLeaseTimeoutMs

proc setMaxAttempts*(n: int) =
  ## Set maximum handler attempts before a job moves to failed state.
  validateMaxAttempts(n)
  queeRegistry.maxAttempts = n

proc maxAttempts*(): int {.gcsafe.} =
  {.cast(gcsafe).}:
    queeRegistry.maxAttempts

proc setRetryDelay*(ms: int) =
  ## Set initial retry delay in milliseconds. Zero retries immediately.
  validateRetryDelay(ms)
  queeRegistry.retryDelayMs = ms

proc retryDelay*(): int {.gcsafe.} =
  {.cast(gcsafe).}:
    queeRegistry.retryDelayMs

proc setRetryBackoff*(factor: float) =
  ## Set exponential retry backoff factor. Minimum 1.0.
  validateRetryBackoff(factor)
  queeRegistry.retryBackoff = factor

proc retryBackoff*(): float {.gcsafe.} =
  {.cast(gcsafe).}:
    queeRegistry.retryBackoff

proc newBackend*(kind: BackendKind): QueueBackend =
  case kind
  of bkSqlite:
    newSqliteBackend()
  of bkMemory:
    newMemoryBackend()

proc currentBackend*(): QueueBackend {.gcsafe.} =
  {.cast(gcsafe).}:
    if activeBackend == nil:
      raise backendNotConfigured()
    activeBackend

proc closeQueueDatabases*() =
  ## Compatibility alias: close the currently configured backend.
  if activeBackend != nil:
    activeBackend.close()

proc markJobRunning*(jobId: string; taskName: string; queue: string; leaseId: string) {.gcsafe.} =
  {.cast(gcsafe).}:
    currentJobId = jobId
    currentQueue = queue
    currentLeaseId = leaseId
    withCancellationLock:
      runningJobs[jobId] = true
      if taskName.len > 0:
        runningTaskCounts[taskName] = runningTaskCounts.getOrDefault(taskName, 0) + 1

proc unmarkJobRunning*(jobId: string; taskName: string) {.gcsafe.} =
  {.cast(gcsafe).}:
    currentJobId = ""
    currentQueue = ""
    currentLeaseId = ""
    withCancellationLock:
      runningJobs.del(jobId)
      if taskName.len > 0 and taskName in runningTaskCounts:
        let next = runningTaskCounts[taskName] - 1
        if next <= 0:
          runningTaskCounts.del(taskName)
        else:
          runningTaskCounts[taskName] = next

proc currentJob*(): string {.gcsafe.} =
  currentJobId

proc renewLease*(): bool {.gcsafe.} =
  ## Extend the current job's lease by ``jobLeaseTimeout``. Intended for long
  ## task handlers; returns false when called outside a running job or after a
  ## newer retry lease has taken ownership.
  let jobId = currentJobId
  let queue = currentQueue
  let leaseId = currentLeaseId
  if jobId.len == 0 or queue.len == 0 or leaseId.len == 0:
    return false
  withQueeDbLock:
    result = currentBackend().renewLease(queue, jobId, leaseId, jobLeaseTimeout())

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

proc managementQueues(queue: string): seq[string] =
  if queue.len > 0:
    if queue notin queeRegistry.queuePaths:
      raise newException(ValueError, "Unknown queue: '" & queue & "'")
    result.add(queue)
  else:
    result = queeRegistry.queues

proc blockedTaskNames*(): seq[string] {.gcsafe.} =
  ## Task names whose per-task concurrency limit is currently full.
  {.cast(gcsafe).}:
    withCancellationLock:
      for taskName, info in queeRegistry.tasks:
        if info.taskConcurrency > UnlimitedTaskConcurrency and
            runningTaskCounts.getOrDefault(taskName, 0) >= info.taskConcurrency:
          result.add(taskName)

proc cancelJob*(jobId: string; queue: string = ""): bool =
  ## Cancel a queued/scheduled job by id.
  ##
  ## If the job has not started yet, it is removed from the backend and will not run.
  ## If it is already running, cancellation is cooperative: the handler should
  ## check ``cancellationRequested()`` and return early.
  if jobId.len == 0:
    return false

  let queues = managementQueues(queue)

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

proc listFailedJobs*(queue: string = ""): seq[FailedJob] =
  ## List jobs that exhausted their retry budget and remain in failed state.
  ##
  ## When ``queue`` is empty, all configured queues are searched.
  let queues = managementQueues(queue)
  withQueeDbLock:
    let backend = currentBackend()
    for queueName in queues:
      result.add(backend.listFailed(queueName))

proc retryFailedJob*(jobId: string; queue: string = ""): bool =
  ## Move a failed job back to the queued state with a fresh retry budget.
  ##
  ## When ``queue`` is empty, all configured queues are searched.
  if jobId.len == 0:
    return false

  let queues = managementQueues(queue)
  withQueeDbLock:
    let backend = currentBackend()
    for queueName in queues:
      if backend.retryFailed(queueName, jobId):
        return true

proc deleteFailedJob*(jobId: string; queue: string = ""): bool =
  ## Delete a failed job without retrying it.
  ##
  ## When ``queue`` is empty, all configured queues are searched.
  if jobId.len == 0:
    return false

  let queues = managementQueues(queue)
  withQueeDbLock:
    let backend = currentBackend()
    for queueName in queues:
      if backend.deleteFailed(queueName, jobId):
        return true

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
  jobLeaseTimeoutMs: int = DefaultJobLeaseTimeoutMs,
  maxAttempts: int = DefaultMaxAttempts,
  retryDelayMs: int = DefaultRetryDelayMs,
  retryBackoff: float = DefaultRetryBackoff,
  skipMissedJobs: bool = false,
  backendKind: BackendKind = bkSqlite,
  backend: QueueBackend = nil,
) =
  ## Open or create the job store using the selected or provided backend.
  ##
  ## When ``skipMissedJobs`` is true, jobs already due at startup are skipped:
  ## one-shot jobs are deleted and recurring jobs are advanced to their next run.
  validatePollInterval(pollIntervalMs)
  validateWorkerConcurrency(workerConcurrency)
  validateJobLeaseTimeout(jobLeaseTimeoutMs)
  validateMaxAttempts(maxAttempts)
  validateRetryDelay(retryDelayMs)
  validateRetryBackoff(retryBackoff)
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
  queeRegistry.jobLeaseTimeoutMs = jobLeaseTimeoutMs
  queeRegistry.maxAttempts = maxAttempts
  queeRegistry.retryDelayMs = retryDelayMs
  queeRegistry.retryBackoff = retryBackoff

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
  name: string;
  defaultQueue: string;
  defaultPriority: int;
  taskConcurrency: int;
  handler: TaskHandler;
) =
  ## Register a named task with default queue and priority (1 = highest, 10 = lowest).
  validatePriority(defaultPriority)
  validateTaskConcurrency(taskConcurrency)
  queeRegistry.handlers[name] = handler
  if name notin queeRegistry.taskOrder:
    queeRegistry.taskOrder.add(name)
  queeRegistry.tasks[name] = TaskInfo(
    name: name,
    defaultQueue: defaultQueue,
    defaultPriority: defaultPriority,
    taskConcurrency: taskConcurrency,
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
      fgCyan, "  concurrency: ", resetStyle,
      fgYellow,
      (if info.taskConcurrency == UnlimitedTaskConcurrency: "unlimited" else: $info.taskConcurrency),
      resetStyle,
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
  queeRegistry.jobLeaseTimeoutMs = DefaultJobLeaseTimeoutMs
  queeRegistry.maxAttempts = DefaultMaxAttempts
  queeRegistry.retryDelayMs = DefaultRetryDelayMs
  queeRegistry.retryBackoff = DefaultRetryBackoff
  withCancellationLock:
    cancelledJobs.clear()
    runningJobs.clear()
    runningTaskCounts.clear()

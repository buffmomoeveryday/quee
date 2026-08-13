# Quee

A lightweight background job queue for Nim. Define tasks with a macro, enqueue work with a fluent API, and let a background worker persist and run jobs through a pluggable storage backend.

## Features

- **Task macro** — declare handlers with typed parameters
- **Fluent scheduling** — run now, delay, intervals, cron, daily/weekly
- **Durable storage** — jobs survive process restarts with the built-in SQLite backend
- **At-least-once delivery** — claimed jobs use leases, bounded retries, backoff, and failed state
- **In-memory storage** — run without disk persistence for tests and ephemeral workloads
- **Pluggable backends** — provide a `QueueBackend` for Redis, Postgres, or another store
- **Background workers** — configurable thread pool (`concurrency`, like Celery `-c`) and poll interval
- **HTTP-friendly** — works with frameworks like [mummy](https://github.com/nim-lang/mummy) (see below)

## Requirements

- Nim >= 2.2.6
- Compile with `--threads:on` (worker uses `createThread`)

## Install

```bash
nimble install quee
```

Or add to your `.nimble`:

```nim
requires "quee"
```

## Quick start

```nim
import quee
import times

initQuee("./mydb")

task sendEmail(email: string, subject: string):
  echo "→ Sending '", subject, "' to ", email

# Run as soon as the worker picks it up (~poll interval)
discard sendEmail.enqueue("a@b.com", "Hello").run()

startQuee()          # start background worker
waitForQuee()        # block main thread (CLI / scheduler apps)
```

For a web server, call `startQuee()` **before** the blocking `serve()` call so the worker is running while requests are handled.

## Defining tasks

The `task` macro registers a handler and creates a task value you enqueue against:

```nim
task sendEmail(email: string, subject: string):
  echo subject, " → ", email

task ping():   # no parameters
  echo "pong"

# Generated roughly as:
#   type SendEmailTask = object
#   proc enqueue(self: SendEmailTask; email, subject: string): JobBuilder
#   let sendEmail = SendEmailTask()
```

Arguments are serialized to JSON when enqueued and unmarshalled in the handler.

### Task metadata

Leading lines in the task body can set queue, priority, and per-task concurrency:

```nim
initQuee("./mydb", queues = ["default", "emails", "urgent"])

task sendEmail(email: string):
  queue "emails"
  priority 5
  concurrency 2
  echo email
```

`concurrency 2` means at most two instances of that task handler run at the same time. If omitted, the task has no per-task cap beyond the worker pool size.

### Tasks in other modules

Tasks can live outside your main module. Define them normally, import that module from your app, and enqueue the generated task value:

```nim
# tasks/email_tasks.nim
import quee

task sendEmail*(email: string):
  echo email

# main.nim
import quee
import tasks/email_tasks

initQuee("./mydb")
discard sendEmail.enqueue("a@b.com").run()
```

Importing the task module also runs the generated `registerTask(...)` call, so the worker knows about the handler.

### Override queue when enqueueing

```nim
discard sendEmail.enqueue("a@b.com", queue = "urgent").run()
```

Resolution: ``queue =`` at enqueue → task's ``queue "…"`` line → `"default"`.

With the default SQLite backend, all queues are stored in `{dbPath}/quee.sqlite3`. The worker
polls every queue round-robin.

## Storage Backends

SQLite is the default backend and stores all queues in `{path}/quee.sqlite3`:

```nim
initQuee("./mydb")
# same as:
initQuee("./mydb", backendKind = bkSqlite)
```

The in-memory backend stores nothing on disk and loses all queued jobs when it is closed or the
process exits:

```nim
initQuee("unused", backendKind = bkMemory)
```

The SQLite backend applies these pragmas on open:

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 5000;
PRAGMA temp_store = MEMORY;
```

### Delivery guarantee

With the SQLite backend, queued jobs are persisted until a worker leases them. A successful handler completes the lease and deletes the one-shot job. A failed handler records the exception message and retries the job after the configured delay/backoff. If attempts are exhausted, the job moves to `failed`. If a process crashes after leasing a job but before completion, another worker can claim it after the lease timeout.

This is an **at-least-once** guarantee: a job should not be lost after it is durably enqueued, but it may run more than once after a crash, timeout, or retry. Task handlers should therefore be idempotent when they perform external side effects.

The default lease timeout is 30 seconds. Set it above the longest expected handler runtime, or call `renewLease()` inside long-running handlers:

```nim
initQuee("./mydb", jobLeaseTimeoutMs = 120_000)
setJobLeaseTimeout(60_000)

task longImport():
  for chunk in chunks:
    process(chunk)
    discard renewLease()
```

The default retry policy is 3 attempts, 1 second initial delay, and 2x exponential backoff:

```nim
initQuee(
  "./mydb",
  maxAttempts = 5,
  retryDelayMs = 2_000,
  retryBackoff = 2.0,
)

setMaxAttempts(5)
setRetryDelay(2_000)
setRetryBackoff(2.0)
```

The in-memory backend follows the same lease/retry lifecycle while the process is alive, but it is not durable across process exits.

Custom backends subclass `QueueBackend` and pass an instance to `initQuee`. `claimDue` must atomically lease one due job so concurrent workers cannot run the same payload at the same time. Leased jobs must become claimable again after `leaseTimeoutMs` unless `complete` or `renewLease` is called:

```nim
import std/json
import quee

type RedisBackend = ref object of QueueBackend
  # client, prefix, etc.

method setup(backend: RedisBackend; basePath: string; queues: openArray[string]) {.gcsafe.} =
  discard

method storagePath(backend: RedisBackend; queue: string): string {.gcsafe.} =
  "redis://" & queue

method enqueue(backend: RedisBackend; queue: string; payload: JsonNode) {.gcsafe.} =
  discard # write payload using jobStorageKey(payload) or an equivalent ordered key

method claimDue(
  backend: RedisBackend; queue: string; blockedTasks: openArray[string]; leaseTimeoutMs: int
): ClaimedJob {.gcsafe.} =
  discard # atomically lease one due job, skipping blockedTasks; return default when none is due

method complete(
  backend: RedisBackend; queue: string; jobId: string; leaseId: string; nextPayload: JsonNode = nil
) {.gcsafe.} =
  discard # delete the leased job, or atomically replace it with nextPayload for recurring jobs

method release(backend: RedisBackend; queue: string; jobId: string; leaseId: string) {.gcsafe.} =
  discard # make a failed leased job immediately claimable again

method fail(
  backend: RedisBackend;
  queue: string;
  jobId: string;
  leaseId: string;
  maxAttempts: int;
  retryDelayMs: int;
  retryBackoff: float;
  errorMessage: string;
) {.gcsafe.} =
  discard # retry with backoff while attempts remain; otherwise mark failed

method renewLease(
  backend: RedisBackend; queue: string; jobId: string; leaseId: string; leaseTimeoutMs: int
): bool {.gcsafe.} =
  discard # extend the lease only if leaseId still owns the job

initQuee("./mydb", backend = RedisBackend())
```

See `src/examples/backendExample.nim` for both built-in backends.

### Job priority

Priority is an integer from **1 (highest)** to **10 (lowest)**. Default is **5** if unset.

Set on the task body (optional, after `queue` if both are used):

```nim
task alert(msg: string):
  queue "urgent"
  priority 1
  echo msg
```

Override when enqueueing or on the builder chain:

```nim
discard alert.enqueue("disk full", priority = 2).run()
discard alert.enqueue("notice").priority(8).run()
```

Resolution: ``priority =`` / ``.priority()`` → task's ``priority N`` line → `5`.

Among jobs that are due in the same queue, the worker runs the **lowest number** first.

## Enqueue & run

| Goal | Code |
|------|------|
| Run ASAP | `discard task.enqueue(args).run()` |
| Set priority | `discard task.enqueue(args, priority = 3).run()` or `.priority(3).run()` |
| Run after delay | `discard task.enqueue(args).after(5.seconds)` |
| Run at a specific time | `discard task.enqueue(args).at(someDateTime)` |
| Repeat every interval | `discard task.enqueue(args).every(1.seconds)` |
| Repeat every 5s (Duration) | `discard task.enqueue(args).every(initDuration(seconds = 5))` |

`enqueue(...)` only builds an in-memory job. A terminal method (`.run()`, `.after()`, `.every(...)`, etc.) writes it to the database.

### `.run()`

Persists the job with `runAt = 0` so the worker treats it as due immediately:

```nim
discard sendEmail.enqueue(email, "Welcome!").run()
```

The worker polls every **200ms** by default. Configure via ``initQuee``, ``setPollInterval``, or ``startQuee(ms)`` — see [Poll interval](#poll-interval) below.

## Scheduling chains

For cron and calendar-based schedules, start with `.schedule()`:

```nim
# Cron (5 fields: minute hour day month weekday)
discard sendEmail.enqueue(e, s).schedule().cron().expr("0 9 * * *")

# Every day at 09:00
discard sendEmail.enqueue(e, s).schedule().every().day().at(nineAm)

# Every Wednesday at 09:00
discard sendEmail.enqueue(e, s).schedule().every().wednesday.at(nineAm)

# Every Wednesday at midnight
discard sendEmail.enqueue(e, s).schedule().every().wednesday.at()

# Every week on today's weekday
discard sendEmail.enqueue(e, s).schedule().every().week().at(nineAm)
```

Cron supports `*`, exact values, and `*/N` step syntax per field.

Recurring jobs (interval, daily, weekly, cron) are **re-queued automatically** after each run. One-shot jobs (`.run()`, `.after()`, `.at()`) are removed after execution.

## Cancellation

Queued or scheduled jobs can be cancelled before they start:

```nim
let job = sendEmail.enqueue("a@b.com", "Welcome").after(5.minutes)
discard job.cancel()

# Or cancel later by id:
discard cancelJob(job.id)
```

Running jobs use cooperative cancellation. The worker records the cancellation request, and the task should check `cancellationRequested()` at safe points:

```nim
task longJob():
  for step in 1 .. 100:
    if cancellationRequested():
      return
    doOneStep(step)
```

Quee does not forcibly kill a running thread; handlers must return voluntarily.

## Poll interval

How long the worker sleeps when no due jobs are found (milliseconds). Default: **200**.

```nim
initQuee("./mydb", pollIntervalMs = 500)

setPollInterval(100)   # before or after startQuee; takes effect on next idle sleep
echo pollInterval()    # current value

startQuee()            # uses interval from initQuee / setPollInterval
startQuee(50)          # override to 50 ms for this start (also updates stored interval)
```

Lower values mean faster pickup of new jobs but more CPU wakeups.

## Skipping missed jobs on deploy

By default, jobs that became due while the app was stopped run when the worker starts again. For deploys where old due work should not catch up, initialize with `skipMissedJobs = true`:

```nim
initQuee("./mydb", skipMissedJobs = true)
startQuee()
```

One-shot jobs that are already due are deleted. Recurring jobs that are already due are advanced to their next run time.

## Concurrency

How many worker threads run jobs in parallel. Default: **1** (same as before). Similar to Celery's `celery -A app worker --concurrency=4`.

```nim
initQuee("./mydb", workerConcurrency = 4)

setWorkerConcurrency(2)   # before startQuee

startQuee()                      # uses value from initQuee / setWorkerConcurrency
startQuee(concurrency = 4)       # override for this start
startQuee(pollIntervalMs = 50, concurrency = 4)
```

Each thread runs the same loop: atomically claim a due job from the configured backend, run the handler, repeat. With worker `concurrency > 1`, multiple handlers can run at once when multiple jobs are due. A task body can add `concurrency N` to cap that specific task below the worker pool size.

**Thread safety:** when `concurrency > 1`, task handlers must not mutate shared global state without synchronization (same expectation as Celery prefork/thread pools).

Calling `startQuee()` twice without `waitForQuee()` raises an error. Multi-process scaling (several app instances sharing `./mydb`) is possible but not managed by Quee.

## API reference

### Setup

```nim
proc initQuee*(
  path: string;
  queues = ["default"];
  pollIntervalMs = 200;
  workerConcurrency = 1;
  jobLeaseTimeoutMs = 30_000;
  maxAttempts = 3;
  retryDelayMs = 1_000;
  retryBackoff = 2.0;
  skipMissedJobs = false;
  backendKind = bkSqlite;
  backend: QueueBackend = nil
)
  ## Create/open job storage with a built-in or custom backend.

proc discardMissedJobs*(): int
  ## Delete due one-shot jobs and advance due recurring jobs without running them.

proc cancelJob*(jobId: string; queue = ""): bool
  ## Cancel a queued job, or request cooperative cancellation for a running job.

proc listFailedJobs*(queue = ""): seq[FailedJob]
  ## List terminal failed jobs. Empty queue searches all queues.

proc retryFailedJob*(jobId: string; queue = ""): bool
  ## Move a failed job back to queued state with a fresh retry budget.

proc deleteFailedJob*(jobId: string; queue = ""): bool
  ## Delete a failed job without retrying it.

proc cancellationRequested*(): bool
  ## True inside a running task after cancellation has been requested.

proc renewLease*(): bool
  ## Extend the current running job's lease. Returns false outside a running job
  ## or when this handler no longer owns the active lease.

proc setPollInterval*(ms: int)
  ## Set worker sleep when idle (ms). Minimum 1.

proc pollInterval*(): int
  ## Current poll interval in milliseconds.

proc setJobLeaseTimeout*(ms: int)
  ## Set claimed-job lease timeout in milliseconds. Minimum 1.

proc jobLeaseTimeout*(): int
  ## Current job lease timeout in milliseconds.

proc setMaxAttempts*(n: int)
  ## Set maximum handler attempts before failed state. Minimum 1.

proc maxAttempts*(): int

proc setRetryDelay*(ms: int)
  ## Set initial retry delay in milliseconds. Minimum 0.

proc retryDelay*(): int

proc setRetryBackoff*(factor: float)
  ## Set exponential retry backoff factor. Minimum 1.0.

proc retryBackoff*(): float

proc setWorkerConcurrency*(n: int)
  ## Worker threads for next ``startQuee`` (1..64).

proc workerConcurrency*(): int

proc startQuee*(pollIntervalMs: int = 0; concurrency: int = 0)
  ## Start worker thread(s). Named ``concurrency`` sets parallel workers (Celery ``-c``).

proc waitForQuee*()
  ## Block until all worker threads exit (runs forever in normal use).
```

### Backends

```nim
type BackendKind = enum bkSqlite, bkMemory
type ClaimedJob = object
  id: string
  leaseId: string
  payload: JsonNode
type FailedJob = object
  id: string
  queue: string
  taskName: string
  attempts: int
  lastError: string
  payload: JsonNode
type QueueBackend = ref object of RootObj

method setup(backend: QueueBackend; basePath: string; queues: openArray[string])
method close(backend: QueueBackend)
method storagePath(backend: QueueBackend; queue: string): string
method enqueue(backend: QueueBackend; queue: string; payload: JsonNode)
method claimDue(
  backend: QueueBackend; queue: string; blockedTasks: openArray[string]; leaseTimeoutMs: int
): ClaimedJob
method requeue(backend: QueueBackend; queue: string; payload: JsonNode)
method complete(backend: QueueBackend; queue: string; jobId: string; leaseId: string; nextPayload: JsonNode = nil)
method release(backend: QueueBackend; queue: string; jobId: string; leaseId: string)
method fail(
  backend: QueueBackend;
  queue: string;
  jobId: string;
  leaseId: string;
  maxAttempts: int;
  retryDelayMs: int;
  retryBackoff: float;
  errorMessage: string;
)
method renewLease(
  backend: QueueBackend; queue: string; jobId: string; leaseId: string; leaseTimeoutMs: int
): bool
method cancel(backend: QueueBackend; queue: string; jobId: string): bool
method listFailed(backend: QueueBackend; queue: string): seq[FailedJob]
method retryFailed(backend: QueueBackend; queue: string; jobId: string): bool
method deleteFailed(backend: QueueBackend; queue: string; jobId: string): bool
method discardMissed(backend: QueueBackend; queue: string): int

proc newSqliteBackend(): SqliteBackend
proc newMemoryBackend(): MemoryBackend
proc jobStorageKey(payload: JsonNode): string
```

### Job builder (from `task.enqueue`)

```nim
proc priority*(b: JobBuilder; p: int): JobBuilder   # 1 = highest, 10 = lowest
proc id*(b: JobBuilder): string
proc cancel*(b: JobBuilder): bool
proc run*(b: JobBuilder): JobBuilder          # run ASAP
proc after*(b: JobBuilder; delay: TimeInterval): JobBuilder
proc at*(b: JobBuilder; time: DateTime): JobBuilder
proc every*(b: JobBuilder; interval: TimeInterval): JobBuilder
proc every*(b: JobBuilder; interval: Duration): JobBuilder
proc schedule*(b: JobBuilder): RunBuilder     # → cron / calendar chains
```

### Calendar / cron (after `.schedule()`)

```nim
proc cron*(b: RunBuilder): CronBuilder
proc expr*(b: CronBuilder; expression: string): JobBuilder

proc every*(b: RunBuilder): EveryBuilder
proc day*(b: EveryBuilder): DayBuilder
proc week*(b: EveryBuilder): WeekdayBuilder
proc monday* … sunday*(b: EveryBuilder): WeekdayBuilder

proc at*(d: DayBuilder; time: DateTime): JobBuilder
proc at*(d: DayBuilder): JobBuilder                    # midnight
proc at*(w: WeekdayBuilder; time: DateTime): JobBuilder
proc at*(w: WeekdayBuilder): JobBuilder                # midnight
```

## HTTP handlers (mummy)

Mummy requires `{.gcsafe.}` route handlers. Quee task values are empty objects (not GC-heavy globals), but your handler still calls into the queue. Wrap registration once:

```nim
import mummy, mummy/routers

proc toGcsafeHandler(h: proc(request: Request)): proc(request: Request) {.gcsafe.} =
  proc (request: Request) {.gcsafe.} =
    {.cast(gcsafe).}:
      h(request)

proc myHandler(request: Request) =
  discard sendEmail.enqueue(request.pathParams["email"], "Hi").run()
  request.respond(200, headers, "ok")

router.get("/objects/@email", toGcsafeHandler(myHandler))

startQuee()   # before serve()
server.serve(Port(8080))
```

See `examples/mummyWebServerExample.nim` for a full example.

## Benchmarks

Measure enqueue throughput, single-threaded drain, and worker end-to-end performance:

```bash
nimble bench
# or with options:
nim c --threads:on --mm:arc --path:src -o:benchmarks/bench_quee benchmarks/bench_quee.nim
./benchmarks/bench_quee --jobs=5000 --concurrency=1,4,8 --poll-ms=10
./benchmarks/bench_quee --drain-jobs=200
```

| Scenario | What it measures |
|----------|------------------|
| **Enqueue (.run)** | Persisting jobs to the configured backend via `enqueue().run()` |
| **processOne drain** | Claiming and running jobs in the main thread (no worker pool) |
| **Worker E2E** | Full pipeline with `startQuee(concurrency=N)` until all jobs finish |

Example output:

```text
=== Enqueue (.run) ===
  jobs:       1000
  elapsed:    245.32 ms
  throughput: 4076 jobs/s

=== Worker E2E (concurrency=4) ===
  jobs:       1000
  elapsed:    312.10 ms
  throughput: 3204 jobs/s
```

Results depend on backend/disk speed, CPU, and `--threads:on`. Use the same machine to compare concurrency settings.

## Examples

Compile from the **project root** (so `import quee` resolves via `--path:src`):

```bash
nimble buildScheduler   # or buildServer
# manually:
nim c --threads:on --mm:arc --path:src src/examples/exampleScheduler.nim
```

### Example index

- `src/examples/basicUsage.nim` — tasks, arguments, immediate work, delayed work, and `processOne()`.
- `src/examples/priorityExample.nim` — default priority, runtime priority override, and priority order.
- `src/examples/queuesExample.nim` — multiple queues, task queue defaults, runtime queue override, and task listing.
- `src/examples/workerConfigExample.nim` — poll interval and concurrency configuration.
- `src/examples/deployUpdateExample.nim` — skip missed jobs on app restart/deploy.
- `src/examples/cancellationExample.nim` — cancel queued jobs and cooperatively stop a running job.
- `src/examples/backendExample.nim` — switch between the SQLite and in-memory backends.
- `src/examples/concurrencyExample.nim` — background worker concurrency with slow simulated work.
- `src/examples/exampleScheduler.nim` — long-running SQLite-backed scheduler with interval, delayed, daily, weekly, queue, priority, poll interval, and concurrency usage.
- `src/examples/mummyWebServerExample.nim` — enqueue background work from a Mummy HTTP handler.

### Background scheduler

`src/examples/exampleScheduler.nim`:

```bash
./src/examples/exampleScheduler
```

### HTTP + enqueue on request

`src/examples/mummyWebServerExample.nim`:

```bash
./src/examples/mummyWebServerExample
curl http://localhost:8080/objects/you@example.com
```

## Project layout

```
src/
  quee.nim              # public entry — re-exports all modules
  quee/
    types.nim           # TaskHandler, JobBuilder, schedules
    registry.nim        # initQuee, registerHandler, global state
    backend.nim         # pluggable storage backend interface
    sqlitebackend.nim   # durable SQLite backend
    memorybackend.nim   # ephemeral in-memory backend
    schedule.nim        # cron, run-at math, isJobDue
    builder.nim         # enqueue fluent API
    worker.nim          # background thread, processOne
    taskmacro.nim       # task macro
  examples/
    basicUsage.nim
    priorityExample.nim
    queuesExample.nim
    workerConfigExample.nim
    deployUpdateExample.nim
    cancellationExample.nim
    backendExample.nim
    concurrencyExample.nim
    exampleScheduler.nim
    mummyWebServerExample.nim
```

## How it works

```mermaid
flowchart TB
  subgraph setup["Setup"]
    INIT["initQuee(path, queues)"]
    TASK["task myJob(...):\n  queue \"emails\"\n  priority 3\n  ...logic..."]
    INIT --> STORE["configured backend\nSQLite, memory, custom"]
    TASK --> REG["registerTask(name, queue, handler)"]
  end

  subgraph enqueue["Enqueue (fluent API)"]
    ENQ["myJob.enqueue(args, queue = ?, priority = ?)"]
    BUILD["JobBuilder"]
    RUN[".run() / .after() / .every() / .schedule()..."]
    PERSIST["persistJob → backend"]
    ENQ --> BUILD --> RUN --> PERSIST
  end

  subgraph worker["Workers (startQuee)"]
    PRINT["printRegisteredTasks()"]
    LOOP["N threads: poll queues, claim job"]
    DUE{"due jobs?\npick lowest priority #"}
    EXEC["handler(args)"]
    RECUR{"recurring?"}
    REQUEUE["update runAt & keep job"]
    PRINT --> LOOP --> DUE
    DUE -->|yes| EXEC --> RECUR
    RECUR -->|once| DONE["delete job"]
    RECUR -->|yes| REQUEUE --> LOOP
    DUE -->|no| LOOP
  end

  REG --> PRINT
  PERSIST --> STORE
  STORE --> LOOP
```

On `startQuee()`, Quee prints every registered task and its default queue, then starts the worker:

```text
[Quee] Registered tasks (2):
  • echoEverySecond  → queue: fast
  • echoEveryDay     → queue: slow
  # output also includes priority and task concurrency
[Quee] Background worker started (concurrency 4, poll 200ms, queues: default, fast, slow)
```

You can also call `printRegisteredTasks()` yourself (e.g. right after your `task` definitions) without starting the worker.

Each job stores:

- `taskName`, `queue`, `args` (JSON), `runAt` (unix timestamp)
- `schedule` (kind: once | cron | every day/week | interval)

The worker deletes one-shot jobs after success and updates `runAt` for recurring jobs.

## Tips

- **Stale database** — delete `./mydb` if you changed job schema during development.
- **Threads** — always compile with `--threads:on`.
- **Order** — `startQuee()` before blocking server `serve()`.
- **ARC/ORC** — examples use `--mm:arc`; default ORC is fine too.

## License

MIT

# Quee

A lightweight background job queue for Nim. Define tasks with a macro, enqueue work with a fluent API, and let a background worker persist and run jobs via [limdb](https://github.com/nim-lang/nimble) (LMDB).

## Features

- **Task macro** — declare handlers with typed parameters
- **Fluent scheduling** — run now, delay, intervals, cron, daily/weekly
- **Durable storage** — jobs survive process restarts (LMDB on disk)
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

### Default queue on the task

First line in the task body can pin a queue (string literal). Use `queue "name"` (`using` is reserved as a Nim keyword in statement position)::

```nim
initQuee("./mydb", queues = ["default", "emails", "urgent"])

task sendEmail(email: string):
  queue "emails"
  echo email
```

### Override queue when enqueueing

```nim
discard sendEmail.enqueue("a@b.com", queue = "urgent").run()
```

Resolution: ``queue =`` at enqueue → task's ``queue "…"`` line → `"default"`.

Each queue is stored under `{dbPath}/{queueName}/` (separate LMDB). The worker polls every queue round-robin.

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

Each thread runs the same loop: claim a due job from LMDB (delete in a transaction), run the handler, repeat. With `concurrency > 1`, multiple handlers can run at once when multiple jobs are due.

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
  skipMissedJobs = false,
)
  ## Create/open the LMDB directory for job storage.

proc discardMissedJobs*(): int
  ## Delete due one-shot jobs and advance due recurring jobs without running them.

proc setPollInterval*(ms: int)
  ## Set worker sleep when idle (ms). Minimum 1.

proc pollInterval*(): int
  ## Current poll interval in milliseconds.

proc setWorkerConcurrency*(n: int)
  ## Worker threads for next ``startQuee`` (1..64).

proc workerConcurrency*(): int

proc startQuee*(pollIntervalMs: int = 0; concurrency: int = 0)
  ## Start worker thread(s). Named ``concurrency`` sets parallel workers (Celery ``-c``).

proc waitForQuee*()
  ## Block until all worker threads exit (runs forever in normal use).
```

### Job builder (from `task.enqueue`)

```nim
proc priority*(b: JobBuilder; p: int): JobBuilder   # 1 = highest, 10 = lowest
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
| **Enqueue (.run)** | Persisting jobs to LMDB via `enqueue().run()` |
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

Results depend on disk speed (LMDB), CPU, and `--threads:on`. Use the same machine to compare concurrency settings.

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
- `src/examples/concurrencyExample.nim` — background worker concurrency with slow simulated work.
- `src/examples/exampleScheduler.nim` — long-running scheduler with interval, delayed, daily, weekly, queue, priority, poll interval, and concurrency usage.
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
    INIT --> DIRS["LMDB dirs per queue\n./mydb/default, ./emails, ..."]
    TASK --> REG["registerTask(name, queue, handler)"]
  end

  subgraph enqueue["Enqueue (fluent API)"]
    ENQ["myJob.enqueue(args, queue = ?, priority = ?)"]
    BUILD["JobBuilder"]
    RUN[".run() / .after() / .every() / .schedule()..."]
    PERSIST["persistJob → LMDB"]
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
  PERSIST --> DIRS
  DIRS --> LOOP
```

On `startQuee()`, Quee prints every registered task and its default queue, then starts the worker:

```text
[Quee] Registered tasks (2):
  • echoEverySecond  → queue: fast
  • echoEveryDay     → queue: slow
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

## Quee benchmarks — enqueue, drain, and worker end-to-end throughput.
##
##   nim c --threads:on --mm:arc --path:src benchmarks/bench_quee.nim
##   ./bench_quee --jobs=1000 --concurrency=1,2,4

import std/[locks, monotimes, os, osproc, parseopt, random, strformat, strutils, times]
import quee

type BenchConfig = object
  jobs: int
  drainJobs: int
  workerJobs: int
  slowJobs: int
  slowJobMs: int
  pollMs: int
  concurrencies: seq[int]
  workerOnly: bool
  slowWorkerOnly: bool

var completed = 0
var completedLock: Lock
var slowJobMs = 100

proc resetCompleted() =
  withLock completedLock:
    completed = 0

proc incCompleted() =
  withLock completedLock:
    inc completed

proc completedCount(): int =
  withLock completedLock:
    result = completed

task noop():
  discard

task countJob():
  incCompleted()

task slowJob():
  sleep(slowJobMs)
  incCompleted()

proc benchDbPath(): string =
  let base = getTempDir() / "quee_bench"
  createDir base
  result = base / &"{epochTime()}_{rand(1_000_000)}"

proc teardownBench(path: string) =
  if dirExists(path):
    removeDir path

proc elapsedMs(start: MonoTime): float =
  float((getMonoTime() - start).inNanoseconds) / 1_000_000.0

proc printResult(name: string; jobs: int; ms: float) =
  let rate =
    if ms > 0: jobs.float / (ms / 1000.0)
    else: 0.0
  echo &"=== {name} ==="
  echo &"  jobs:       {jobs}"
  echo &"  elapsed:    {ms:.2f} ms"
  echo &"  throughput: {int(rate)} jobs/s"
  echo ""

proc parseConcurrencies(s: string): seq[int] =
  for part in s.split(','):
    let p = part.strip()
    if p.len > 0:
      result.add parseInt(p)

proc benchExecutable(): string =
  ## Stable path for worker subprocesses (`nim c -r` deletes the binary after start).
  let stable = absolutePath(getCurrentDir() / "benchmarks" / "bench_quee")
  if fileExists(stable):
    return stable
  let self = absolutePath(paramStr(0))
  if fileExists(self) and "(deleted)" notin self:
    return self
  stable

proc parseConfig(): BenchConfig =
  result = BenchConfig(
    jobs: 1000, drainJobs: 0, workerJobs: -1, slowJobs: 0, slowJobMs: 100,
    pollMs: 10, concurrencies: @[1, 2, 4],
  )
  var p = initOptParser()
  for kind, key, val in p.getopt():
    case kind
    of cmdArgument:
      discard
    of cmdLongOption, cmdShortOption:
      case key
      of "jobs", "j":
        result.jobs = parseInt(val)
      of "drain-jobs":
        result.drainJobs = parseInt(val)
      of "worker-jobs":
        result.workerJobs = parseInt(val)
      of "slow-jobs":
        result.slowJobs = parseInt(val)
      of "slow-job-ms":
        result.slowJobMs = parseInt(val)
      of "poll-ms":
        result.pollMs = parseInt(val)
      of "concurrency", "c":
        result.concurrencies = parseConcurrencies(val)
      of "worker-only":
        result.workerOnly = true
      of "slow-worker-only":
        result.slowWorkerOnly = true
      of "help", "h":
        echo "Usage: bench_quee [options]"
        echo "  --jobs=N          Jobs for enqueue benchmark (default: 1000)"
        echo "  --drain-jobs=N    Jobs for processOne drain (default: min(jobs, 200))"
        echo "  --worker-jobs=N   Jobs for tiny-handler E2E, 0 skips it (default: min(jobs, 1000))"
        echo "  --slow-jobs=N     Jobs for slow handler E2E (default: min(jobs, 100))"
        echo "  --slow-job-ms=N   Sleep per slow handler job (default: 100)"
        echo "  --poll-ms=N       Worker poll interval for E2E (default: 10)"
        echo "  --concurrency=S   Comma-separated worker counts for E2E (default: 1,2,4)"
        echo "  --worker-only     Internal: run a single worker E2E scenario"
        echo "  --slow-worker-only Internal: run a single slow worker E2E scenario"
        quit 0
      else:
        discard
    of cmdEnd:
      discard

proc benchEnqueue(jobs: int) =
  let path = benchDbPath()
  defer: teardownBench(path)
  initQuee(path, pollIntervalMs = 10)

  let t0 = getMonoTime()
  for _ in 0 ..< jobs:
    discard noop.enqueue().run()
  printResult("Enqueue (.run)", jobs, elapsedMs(t0))

proc benchDrain(jobs: int) =
  let path = benchDbPath()
  defer: teardownBench(path)
  initQuee(path, pollIntervalMs = 10)

  for _ in 0 ..< jobs:
    discard noop.enqueue().run()

  var processed = 0
  let t0 = getMonoTime()
  while processOne():
    inc processed
  printResult(
    &"processOne drain (n={jobs}, ordered-key claim)",
    processed,
    elapsedMs(t0),
  )

proc waitForCompleted(expected: int; timeoutSec: float): bool =
  let deadline = epochTime() + timeoutSec
  while completedCount() < expected:
    if epochTime() >= deadline:
      return false
    sleep(10)
  true

proc benchWorkerE2E(jobs: int; pollMs: int; concurrency: int) =
  resetCompleted()
  let path = benchDbPath()
  defer: teardownBench(path)
  initQuee(path, pollIntervalMs = pollMs, workerConcurrency = concurrency)

  for _ in 0 ..< jobs:
    discard countJob.enqueue().run()

  let t0 = getMonoTime()
  startQuee(pollIntervalMs = pollMs, concurrency = concurrency)
  if not waitForCompleted(jobs, 60.0):
    echo &"  ERROR: timed out after 60s (completed {completedCount()}/{jobs})"
    quit 1
  printResult(&"Worker E2E (concurrency={concurrency})", jobs, elapsedMs(t0))

proc benchSlowWorkerE2E(jobs: int; pollMs: int; concurrency: int; jobMs: int) =
  resetCompleted()
  slowJobMs = jobMs
  let path = benchDbPath()
  defer: teardownBench(path)
  initQuee(path, pollIntervalMs = pollMs, workerConcurrency = concurrency)

  for _ in 0 ..< jobs:
    discard slowJob.enqueue().run()

  let t0 = getMonoTime()
  startQuee(pollIntervalMs = pollMs, concurrency = concurrency)
  let timeoutSec = max(60.0, (jobs.float * jobMs.float / 1000.0) + 30.0)
  if not waitForCompleted(jobs, timeoutSec):
    echo &"  ERROR: timed out after {timeoutSec:.0f}s (completed {completedCount()}/{jobs})"
    quit 1
  printResult(
    &"Slow Worker E2E (concurrency={concurrency}, job={jobMs}ms)",
    jobs,
    elapsedMs(t0),
  )

proc runWorkerOnly(cfg: BenchConfig) =
  if cfg.concurrencies.len == 0:
    echo "No concurrency value specified"
    quit 1
  let c = cfg.concurrencies[0]
  validateWorkerConcurrency(c)
  benchWorkerE2E(cfg.jobs, cfg.pollMs, c)
  quit 0

proc slowJobCount(cfg: BenchConfig): int =
  if cfg.slowJobs > 0:
    cfg.slowJobs
  else:
    min(cfg.jobs, 100)

proc workerJobCount(cfg: BenchConfig): int =
  if cfg.workerJobs >= 0:
    cfg.workerJobs
  else:
    min(cfg.jobs, 1000)

proc runSlowWorkerOnly(cfg: BenchConfig) =
  if cfg.concurrencies.len == 0:
    echo "No concurrency value specified"
    quit 1
  let c = cfg.concurrencies[0]
  validateWorkerConcurrency(c)
  benchSlowWorkerE2E(slowJobCount(cfg), cfg.pollMs, c, cfg.slowJobMs)
  quit 0

proc drainJobCount(cfg: BenchConfig): int =
  if cfg.drainJobs > 0:
    cfg.drainJobs
  else:
    min(cfg.jobs, 200)

proc runWorkerSubprocess(cfg: BenchConfig; exe: string; concurrency: int) =
  let args = @[
    "--worker-only",
    &"--jobs={workerJobCount(cfg)}",
    &"--poll-ms={cfg.pollMs}",
    &"--concurrency={concurrency}",
  ]
  let p = startProcess(exe, args = args, options = {poParentStreams})
  let code = p.waitForExit()
  if code != 0:
    raise newException(
      ValueError,
      &"Worker E2E subprocess failed for concurrency={concurrency} (exit {code})",
    )

proc runSlowWorkerSubprocess(cfg: BenchConfig; exe: string; concurrency: int) =
  let args = @[
    "--slow-worker-only",
    &"--slow-jobs={slowJobCount(cfg)}",
    &"--slow-job-ms={cfg.slowJobMs}",
    &"--poll-ms={cfg.pollMs}",
    &"--concurrency={concurrency}",
  ]
  let p = startProcess(exe, args = args, options = {poParentStreams})
  let code = p.waitForExit()
  if code != 0:
    raise newException(
      ValueError,
      &"Slow Worker E2E subprocess failed for concurrency={concurrency} (exit {code})",
    )

proc main() =
  initLock(completedLock)
  randomize()
  let cfg = parseConfig()

  if cfg.slowWorkerOnly:
    runSlowWorkerOnly(cfg)
    return

  if cfg.workerOnly:
    runWorkerOnly(cfg)
    return

  let drainN = drainJobCount(cfg)
  let workerN = workerJobCount(cfg)
  let slowN = slowJobCount(cfg)
  echo "Quee benchmarks"
  echo &"  jobs={cfg.jobs}  drain-jobs={drainN}  worker-jobs={workerN}  slow-jobs={slowN}  slow-job-ms={cfg.slowJobMs}  poll-ms={cfg.pollMs}"
  echo &"  concurrency={cfg.concurrencies.join(\",\")}"
  echo ""

  benchEnqueue(cfg.jobs)
  benchDrain(drainN)

  let exe = benchExecutable()
  if not fileExists(exe):
    echo &"Benchmark binary not found at {exe}; run: nim c -o:benchmarks/bench_quee ..."
    quit 1

  if workerN > 0:
    for c in cfg.concurrencies:
      validateWorkerConcurrency(c)
      runWorkerSubprocess(cfg, exe, c)

  for c in cfg.concurrencies:
    validateWorkerConcurrency(c)
    runSlowWorkerSubprocess(cfg, exe, c)

main()

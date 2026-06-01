import std/[json, os, times, unittest]
import quee
import helpers

var backendHits: seq[string] = @[]

task backendLow():
  backendHits.add("low")

task backendHigh():
  priority 1
  backendHits.add("high")

task backendDelayed():
  backendHits.add("delayed")

task backendRecurring():
  backendHits.add("recurring")

type MemoryBackend = ref object of QueueBackend
  basePath: string
  queues: seq[string]
  jobs: seq[tuple[queue: string, payload: JsonNode]]

proc newMemoryBackend(): MemoryBackend =
  MemoryBackend()

method setup(backend: MemoryBackend; basePath: string; queues: openArray[string]) {.gcsafe.} =
  {.cast(gcsafe).}:
    backend.basePath = basePath
    backend.queues = @queues
    backend.jobs = @[]

method storagePath(backend: MemoryBackend; queue: string): string {.gcsafe.} =
  {.cast(gcsafe).}:
    backend.basePath / queue

method enqueue(backend: MemoryBackend; queue: string; payload: JsonNode) {.gcsafe.} =
  {.cast(gcsafe).}:
    backend.jobs.add((queue, payload))

method requeue(backend: MemoryBackend; queue: string; payload: JsonNode) {.gcsafe.} =
  {.cast(gcsafe).}:
    backend.jobs.add((queue, payload))

method claimDue(backend: MemoryBackend; queue: string): ClaimedJob {.gcsafe.} =
  {.cast(gcsafe).}:
    var bestIndex = -1
    var bestPriority = MaxPriority + 1
    for i, item in backend.jobs:
      if item.queue != queue or not isJobDue($item.payload):
        continue
      let pri = jobPriority(item.payload)
      if bestIndex < 0 or pri < bestPriority:
        bestIndex = i
        bestPriority = pri

    if bestIndex >= 0:
      let payload = backend.jobs[bestIndex].payload
      backend.jobs.delete(bestIndex)
      result = ClaimedJob(id: payload["id"].getStr(), payload: payload)

method cancel(backend: MemoryBackend; queue: string; jobId: string): bool {.gcsafe.} =
  {.cast(gcsafe).}:
    for i, item in backend.jobs:
      if item.queue == queue and item.payload["id"].getStr() == jobId:
        backend.jobs.delete(i)
        return true

method discardMissed(backend: MemoryBackend; queue: string): int {.gcsafe.} =
  {.cast(gcsafe).}:
    var kept: seq[tuple[queue: string, payload: JsonNode]] = @[]
    for item in backend.jobs:
      if item.queue != queue or not isJobDue($item.payload):
        kept.add(item)
      else:
        inc result
    backend.jobs = kept

suite "storage backends":
  var dbPath: string

  setup:
    backendHits = @[]
    dbPath = uniqueTestDb()

  teardown:
    teardownQuee(dbPath)

  test "sqlite backend runs and orders jobs":
    initQuee(dbPath, backendKind = bkSqlite)
    discard backendLow.enqueue().run()
    discard backendHigh.enqueue().run()

    check fileExists(dbPath / "quee.sqlite3")
    check processOne()
    check processOne()
    check backendHits == @["high", "low"]

  test "sqlite backend cancels queued jobs":
    initQuee(dbPath, backendKind = bkSqlite)
    let job = backendDelayed.enqueue().after(1.hours)
    check job.cancel()
    sleep(20)
    check not processOne()
    check backendHits.len == 0

  test "sqlite backend skipMissedJobs advances recurring work":
    initQuee(dbPath, backendKind = bkSqlite)
    discard backendRecurring.enqueue().every(100.milliseconds)
    sleep(150)

    initQuee(dbPath, backendKind = bkSqlite, skipMissedJobs = true)
    check not processOne()
    sleep(150)
    check processOne()
    check backendHits == @["recurring"]

  test "custom memory backend can be plugged in":
    let backend = newMemoryBackend()
    initQuee(dbPath, backend = backend)
    discard backendLow.enqueue().run()
    discard backendHigh.enqueue().run()

    check processOne()
    check processOne()
    check backendHits == @["high", "low"]

  test "invalid queues are rejected before backend setup":
    let backend = newMemoryBackend()
    expect ValueError:
      initQuee(dbPath, queues = ["emails"], backend = backend)
    check backend.queues.len == 0

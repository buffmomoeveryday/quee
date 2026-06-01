import std/[json, os, strformat, terminal]
import ../quee

var hits: seq[string] = @[]

task backendLow():
  hits.add("low")

task backendHigh():
  priority 1
  hits.add("high")

type MemoryBackend = ref object of QueueBackend
  basePath: string
  jobs: seq[tuple[queue: string, payload: JsonNode]]

proc newMemoryBackend(): MemoryBackend =
  MemoryBackend()

method setup(backend: MemoryBackend; basePath: string; queues: openArray[string]) {.gcsafe.} =
  {.cast(gcsafe).}:
    backend.basePath = basePath
    backend.jobs = @[]

method storagePath(backend: MemoryBackend; queue: string): string {.gcsafe.} =
  {.cast(gcsafe).}:
    backend.basePath / queue

method enqueue(backend: MemoryBackend; queue: string; payload: JsonNode) {.gcsafe.} =
  {.cast(gcsafe).}:
    backend.jobs.add((queue, payload))

method claimDue(backend: MemoryBackend; queue: string): ClaimedJob {.gcsafe.} =
  {.cast(gcsafe).}:
    var bestIndex = -1
    var bestPriority = MaxPriority + 1

    for i, item in backend.jobs:
      if item.queue != queue or not isJobDue($item.payload):
        continue

      let priority = jobPriority(item.payload)
      if bestIndex < 0 or priority < bestPriority:
        bestIndex = i
        bestPriority = priority

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

proc removeDb(path: string) =
  if dirExists(path):
    removeDir(path)

proc runScenario(name: string) =
  hits = @[]
  discard backendLow.enqueue().run()
  discard backendHigh.enqueue().run()
  discard processOne()
  discard processOne()
  styledEcho fgGreen, name, resetStyle, &" ran jobs in order: {hits}"

proc runBuiltInBackend(name: string; kind: BackendKind) =
  let path = "./mydb_" & name
  removeDb(path)
  initQuee(path, backendKind = kind)
  runScenario(name)
  closeQueueDatabases()
  removeDb(path)

runBuiltInBackend("limdb", bkLimdb)
runBuiltInBackend("sqlite", bkSqlite)

let memoryPath = "./memory-backend"
initQuee(memoryPath, backend = newMemoryBackend())
runScenario("custom memory")
closeQueueDatabases()

import std/[os, strformat, terminal]
import ../quee

var hits: seq[string] = @[]

task backendLow():
  hits.add("low")

task backendHigh():
  priority 1
  hits.add("high")

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

runBuiltInBackend("sqlite", bkSqlite)
runBuiltInBackend("memory", bkMemory)

# Custom backends must also implement saveCheckpoint so a task can resume after
# a crash. Return false when the lease no longer owns the job. See the README
# method next to renewLease:
#
# method saveCheckpoint(
#   backend: RedisBackend; queue, jobId, leaseId, name: string;
#   checkpointResult: JsonNode; leaseTimeoutMs: int
# ): bool {.gcsafe.} =
#   discard # append {"name", "result"} to the leased payload and extend the lease

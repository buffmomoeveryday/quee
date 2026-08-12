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

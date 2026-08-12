import std/[os, strutils, times, unittest]
import db_connector/db_sqlite
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

  test "sqlite backend enables WAL journal mode":
    initQuee(dbPath, backendKind = bkSqlite)
    closeQueueDatabases()

    let db = open(dbPath / "quee.sqlite3", "", "", "")
    defer: db.close()
    check db.getValue(sql"PRAGMA journal_mode").toLowerAscii() == "wal"

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

  test "memory backend runs and orders jobs":
    initQuee(dbPath, backendKind = bkMemory)
    discard backendLow.enqueue().run()
    discard backendHigh.enqueue().run()

    check processOne()
    check processOne()
    check backendHits == @["high", "low"]
    check not dirExists(dbPath)

  test "memory backend cancels queued jobs":
    initQuee(dbPath, backendKind = bkMemory)
    let job = backendDelayed.enqueue().after(1.hours)
    check job.cancel()
    sleep(20)
    check not processOne()
    check backendHits.len == 0

  test "memory backend advances missed recurring work":
    initQuee(dbPath, backendKind = bkMemory)
    discard backendRecurring.enqueue().every(100.milliseconds)
    sleep(150)

    check discardMissedJobs() == 1
    check not processOne()
    sleep(150)
    check processOne()
    check backendHits == @["recurring"]

  test "invalid queues are rejected before backend setup":
    let backend = newMemoryBackend()
    expect ValueError:
      initQuee(dbPath, queues = ["emails"], backend = backend)

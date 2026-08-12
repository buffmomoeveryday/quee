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

task backendFlaky():
  backendHits.add("attempt")
  if backendHits.len == 1:
    raise newException(ValueError, "transient failure")

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

  test "sqlite backend retries jobs after handler failure":
    initQuee(dbPath, backendKind = bkSqlite)
    discard backendFlaky.enqueue().run()

    check processOne()
    check backendHits == @["attempt"]
    check processOne()
    check backendHits == @["attempt", "attempt"]
    check not processOne()

  test "sqlite backend retries expired leased jobs":
    initQuee(dbPath, backendKind = bkSqlite, jobLeaseTimeoutMs = 20)
    let job = backendLow.enqueue().run()
    var claimed: ClaimedJob
    withQueeDbLock:
      claimed = currentBackend().claimDue("default", @[], jobLeaseTimeout())

    check claimed.id == job.id
    check not processOne()
    sleep(40)
    check processOne()
    check backendHits == @["low"]

  test "sqlite backend ignores stale lease completion":
    initQuee(dbPath, backendKind = bkSqlite, jobLeaseTimeoutMs = 20)
    let job = backendLow.enqueue().run()
    var first: ClaimedJob
    var second: ClaimedJob

    withQueeDbLock:
      first = currentBackend().claimDue("default", @[], jobLeaseTimeout())
    sleep(40)
    withQueeDbLock:
      second = currentBackend().claimDue("default", @[], jobLeaseTimeout())
      currentBackend().complete("default", first.id, first.leaseId)
      currentBackend().release("default", second.id, second.leaseId)

    check first.id == job.id
    check second.id == job.id
    check first.leaseId != second.leaseId
    check processOne()
    check backendHits == @["low"]

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

  test "memory backend retries jobs after handler failure":
    initQuee(dbPath, backendKind = bkMemory)
    discard backendFlaky.enqueue().run()

    check processOne()
    check backendHits == @["attempt"]
    check processOne()
    check backendHits == @["attempt", "attempt"]
    check not processOne()

  test "memory backend retries expired leased jobs":
    initQuee(dbPath, backendKind = bkMemory, jobLeaseTimeoutMs = 20)
    let job = backendLow.enqueue().run()
    var claimed: ClaimedJob
    withQueeDbLock:
      claimed = currentBackend().claimDue("default", @[], jobLeaseTimeout())

    check claimed.id == job.id
    check not processOne()
    sleep(40)
    check processOne()
    check backendHits == @["low"]

  test "memory backend ignores stale lease completion":
    initQuee(dbPath, backendKind = bkMemory, jobLeaseTimeoutMs = 20)
    let job = backendLow.enqueue().run()
    var first: ClaimedJob
    var second: ClaimedJob

    withQueeDbLock:
      first = currentBackend().claimDue("default", @[], jobLeaseTimeout())
    sleep(40)
    withQueeDbLock:
      second = currentBackend().claimDue("default", @[], jobLeaseTimeout())
      currentBackend().complete("default", first.id, first.leaseId)
      currentBackend().release("default", second.id, second.leaseId)

    check first.id == job.id
    check second.id == job.id
    check first.leaseId != second.leaseId
    check processOne()
    check backendHits == @["low"]

  test "invalid queues are rejected before backend setup":
    let backend = newMemoryBackend()
    expect ValueError:
      initQuee(dbPath, queues = ["emails"], backend = backend)

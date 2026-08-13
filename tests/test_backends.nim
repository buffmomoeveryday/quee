import std/[json, os, sequtils, strutils, times, unittest]
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

task backendAlwaysFail():
  backendHits.add("fail")
  raise newException(ValueError, "permanent failure")

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
    initQuee(dbPath, backendKind = bkSqlite, retryDelayMs = 0)
    discard backendFlaky.enqueue().run()

    check processOne()
    check backendHits == @["attempt"]
    check processOne()
    check backendHits == @["attempt", "attempt"]
    check not processOne()

  test "sqlite backend moves exhausted retries to failed state":
    initQuee(dbPath, backendKind = bkSqlite, maxAttempts = 2, retryDelayMs = 0)
    discard backendAlwaysFail.enqueue().run()

    check processOne()
    check processOne()
    check not processOne()
    check backendHits == @["fail", "fail"]

    let db = open(dbPath / "quee.sqlite3", "", "", "")
    defer: db.close()
    check db.getValue(sql"SELECT state FROM jobs LIMIT 1") == "failed"
    check db.getValue(sql"SELECT last_error FROM jobs LIMIT 1") == "permanent failure"

  test "sqlite backend lists and deletes failed jobs":
    initQuee(dbPath, backendKind = bkSqlite, maxAttempts = 1, retryDelayMs = 0)
    let job = backendAlwaysFail.enqueue().run()

    check processOne()
    let failed = listFailedJobs()
    check failed.len == 1
    check failed[0].id == job.id
    check failed[0].queue == "default"
    check failed[0].taskName == "backendAlwaysFail"
    check failed[0].attempts == 1
    check failed[0].lastError == "permanent failure"
    check failed[0].payload["id"].getStr() == job.id

    check deleteFailedJob(job.id)
    check listFailedJobs().len == 0
    check not deleteFailedJob(job.id)

  test "sqlite backend exposes job snapshots and queue stats":
    initQuee(dbPath, backendKind = bkSqlite)
    let queuedJob = backendLow.enqueue().run()
    let scheduledJob = backendDelayed.enqueue().after(1.hours)
    var claimed: ClaimedJob
    withQueeDbLock:
      claimed = currentBackend().claimDue("default", @[], jobLeaseTimeout())

    let jobs = listJobs()
    check jobs.len == 2
    check jobs.anyIt(it.id == queuedJob.id and it.state == "running")
    check jobs.anyIt(it.id == scheduledJob.id and it.state == "queued")

    let stats = queueStats()
    check stats.len == 1
    check stats[0].running == 1
    check stats[0].scheduled == 1
    check stats[0].queued == 0
    check claimed.id == queuedJob.id

  test "sqlite backend retries failed jobs through public API":
    initQuee(dbPath, backendKind = bkSqlite, maxAttempts = 1, retryDelayMs = 0)
    let job = backendFlaky.enqueue().run()

    check processOne()
    check listFailedJobs().len == 1
    check retryFailedJob(job.id)
    check listFailedJobs().len == 0
    check processOne()
    check backendHits == @["attempt", "attempt"]
    check not processOne()

  test "sqlite backend manages failed jobs by queue":
    initQuee(dbPath, queues = ["default", "emails"], backendKind = bkSqlite, maxAttempts = 1, retryDelayMs = 0)
    let defaultJob = backendAlwaysFail.enqueue().run()
    let emailJob = backendAlwaysFail.enqueue(queue = "emails").run()

    check processOne()
    check processOne()
    check listFailedJobs().len == 2
    check listFailedJobs(queue = "default").len == 1
    check listFailedJobs(queue = "emails").len == 1
    check listFailedJobs(queue = "default")[0].id == defaultJob.id
    check listFailedJobs(queue = "emails")[0].id == emailJob.id

    check not retryFailedJob(defaultJob.id, queue = "emails")
    check retryFailedJob(emailJob.id, queue = "emails")
    check listFailedJobs(queue = "emails").len == 0
    check listFailedJobs(queue = "default").len == 1

    check not deleteFailedJob(emailJob.id, queue = "emails")
    check deleteFailedJob(defaultJob.id, queue = "default")
    check listFailedJobs().len == 0

  test "sqlite failed-job API handles empty ids and unknown queues":
    initQuee(dbPath, backendKind = bkSqlite)

    check not retryFailedJob("")
    check not deleteFailedJob("")
    expect ValueError:
      discard listFailedJobs(queue = "missing")
    expect ValueError:
      discard retryFailedJob("job", queue = "missing")
    expect ValueError:
      discard deleteFailedJob("job", queue = "missing")

  test "sqlite backend retries expired leased jobs":
    initQuee(dbPath, backendKind = bkSqlite, jobLeaseTimeoutMs = 20)
    let job = backendLow.enqueue().run()
    var claimed: ClaimedJob
    withQueeDbLock:
      claimed = currentBackend().claimDue("default", @[], jobLeaseTimeout())

    check claimed.id == job.id
    check claimed.leaseId.len > 0
    check job.id notin claimed.leaseId
    check not processOne()
    sleep(40)
    check processOne()
    check backendHits == @["low"]

  test "sqlite backend renews active leases":
    initQuee(dbPath, backendKind = bkSqlite, jobLeaseTimeoutMs = 50)
    let job = backendLow.enqueue().run()
    var claimed: ClaimedJob
    withQueeDbLock:
      claimed = currentBackend().claimDue("default", @[], jobLeaseTimeout())

    sleep(30)
    withQueeDbLock:
      check currentBackend().renewLease("default", claimed.id, claimed.leaseId, jobLeaseTimeout())
    sleep(30)
    check not processOne()
    sleep(60)
    check processOne()
    check claimed.id == job.id
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
    initQuee(dbPath, backendKind = bkMemory, retryDelayMs = 0)
    discard backendFlaky.enqueue().run()

    check processOne()
    check backendHits == @["attempt"]
    check processOne()
    check backendHits == @["attempt", "attempt"]
    check not processOne()

  test "memory backend stops exhausted retries":
    initQuee(dbPath, backendKind = bkMemory, maxAttempts = 2, retryDelayMs = 0)
    discard backendAlwaysFail.enqueue().run()

    check processOne()
    check processOne()
    check not processOne()
    check backendHits == @["fail", "fail"]

  test "memory backend lists and deletes failed jobs":
    initQuee(dbPath, backendKind = bkMemory, maxAttempts = 1, retryDelayMs = 0)
    let job = backendAlwaysFail.enqueue().run()

    check processOne()
    let failed = listFailedJobs()
    check failed.len == 1
    check failed[0].id == job.id
    check failed[0].queue == "default"
    check failed[0].taskName == "backendAlwaysFail"
    check failed[0].attempts == 1
    check failed[0].lastError == "permanent failure"
    check failed[0].payload["id"].getStr() == job.id

    check deleteFailedJob(job.id)
    check listFailedJobs().len == 0
    check not deleteFailedJob(job.id)

  test "memory backend exposes job snapshots and queue stats":
    initQuee(dbPath, backendKind = bkMemory)
    let queuedJob = backendLow.enqueue().run()
    let scheduledJob = backendDelayed.enqueue().after(1.hours)
    var claimed: ClaimedJob
    withQueeDbLock:
      claimed = currentBackend().claimDue("default", @[], jobLeaseTimeout())

    let jobs = listJobs()
    check jobs.len == 2
    check jobs.anyIt(it.id == queuedJob.id and it.state == "running")
    check jobs.anyIt(it.id == scheduledJob.id and it.state == "queued")

    let stats = queueStats()
    check stats.len == 1
    check stats[0].running == 1
    check stats[0].scheduled == 1
    check stats[0].queued == 0
    check claimed.id == queuedJob.id

  test "memory backend retries failed jobs through public API":
    initQuee(dbPath, backendKind = bkMemory, maxAttempts = 1, retryDelayMs = 0)
    let job = backendFlaky.enqueue().run()

    check processOne()
    check listFailedJobs().len == 1
    check retryFailedJob(job.id)
    check listFailedJobs().len == 0
    check processOne()
    check backendHits == @["attempt", "attempt"]
    check not processOne()

  test "memory backend manages failed jobs by queue":
    initQuee(dbPath, queues = ["default", "emails"], backendKind = bkMemory, maxAttempts = 1, retryDelayMs = 0)
    let defaultJob = backendAlwaysFail.enqueue().run()
    let emailJob = backendAlwaysFail.enqueue(queue = "emails").run()

    check processOne()
    check processOne()
    check listFailedJobs().len == 2
    check listFailedJobs(queue = "default").len == 1
    check listFailedJobs(queue = "emails").len == 1
    check listFailedJobs(queue = "default")[0].id == defaultJob.id
    check listFailedJobs(queue = "emails")[0].id == emailJob.id

    check not retryFailedJob(defaultJob.id, queue = "emails")
    check retryFailedJob(emailJob.id, queue = "emails")
    check listFailedJobs(queue = "emails").len == 0
    check listFailedJobs(queue = "default").len == 1

    check not deleteFailedJob(emailJob.id, queue = "emails")
    check deleteFailedJob(defaultJob.id, queue = "default")
    check listFailedJobs().len == 0

  test "memory failed-job API handles empty ids and unknown queues":
    initQuee(dbPath, backendKind = bkMemory)

    check not retryFailedJob("")
    check not deleteFailedJob("")
    expect ValueError:
      discard listFailedJobs(queue = "missing")
    expect ValueError:
      discard retryFailedJob("job", queue = "missing")
    expect ValueError:
      discard deleteFailedJob("job", queue = "missing")

  test "memory backend retries expired leased jobs":
    initQuee(dbPath, backendKind = bkMemory, jobLeaseTimeoutMs = 20)
    let job = backendLow.enqueue().run()
    var claimed: ClaimedJob
    withQueeDbLock:
      claimed = currentBackend().claimDue("default", @[], jobLeaseTimeout())

    check claimed.id == job.id
    check claimed.leaseId.len > 0
    check job.id notin claimed.leaseId
    check not processOne()
    sleep(40)
    check processOne()
    check backendHits == @["low"]

  test "memory backend renews active leases":
    initQuee(dbPath, backendKind = bkMemory, jobLeaseTimeoutMs = 50)
    let job = backendLow.enqueue().run()
    var claimed: ClaimedJob
    withQueeDbLock:
      claimed = currentBackend().claimDue("default", @[], jobLeaseTimeout())

    sleep(30)
    withQueeDbLock:
      check currentBackend().renewLease("default", claimed.id, claimed.leaseId, jobLeaseTimeout())
    sleep(30)
    check not processOne()
    sleep(60)
    check processOne()
    check claimed.id == job.id
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

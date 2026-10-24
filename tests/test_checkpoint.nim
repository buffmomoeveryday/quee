import std/[json, os, strutils, times, unittest]
import quee
import helpers

var loadRuns = 0
var writeRuns = 0
var seenCount = 0
var crashBeforeWrite = false
var partialRuns = 0
var afterPartialRuns = 0
var continuePartial = false
var voidRuns = 0
var valueRuns = 0
var seenLabel = ""
var failValues = false
var orderARuns = 0
var orderBRuns = 0
var reorder = false
var recurRuns = 0

task importBatch(batchId: string):
  let count = checkpoint("load"):
    inc loadRuns
    7
  seenCount = count
  if crashBeforeWrite:
    crashBeforeWrite = false
    raise newException(ValueError, "crash after load")
  checkpoint("write"):
    inc writeRuns
    discard batchId

task partialStep():
  checkpoint("partial"):
    inc partialRuns
    if not continuePartial:
      continuePartial = true
      raise newException(ValueError, "crash inside partial")
  checkpoint("after"):
    inc afterPartialRuns

task voidAndValue():
  checkpoint("mark"):
    inc voidRuns
  let label = checkpoint("label"):
    inc valueRuns
    "ok"
  seenLabel = label
  if failValues:
    failValues = false
    raise newException(ValueError, "retry values")

task orderedSteps():
  if not reorder:
    checkpoint("a"):
      inc orderARuns
    checkpoint("b"):
      inc orderBRuns
    reorder = true
    raise newException(ValueError, "reorder")
  checkpoint("b"):
    inc orderBRuns
  checkpoint("a"):
    inc orderARuns

task recurTick():
  checkpoint("tick"):
    inc recurRuns

task nestedSteps():
  checkpoint("outer"):
    checkpoint("inner"):
      discard

task duplicateStep():
  checkpoint("same"):
    discard
  checkpoint("same"):
    discard

task leaseProbe():
  discard

proc checkpointOutside() =
  checkpoint("nope"):
    discard

proc resetCounters() =
  loadRuns = 0
  writeRuns = 0
  seenCount = 0
  crashBeforeWrite = false
  partialRuns = 0
  afterPartialRuns = 0
  continuePartial = false
  voidRuns = 0
  valueRuns = 0
  seenLabel = ""
  failValues = false
  orderARuns = 0
  orderBRuns = 0
  reorder = false
  recurRuns = 0

proc openBackend(kind: BackendKind): string =
  result = uniqueTestDb()
  initQuee(
    result,
    backendKind = kind,
    retryDelayMs = 0,
    maxAttempts = 4,
    jobLeaseTimeoutMs = 1_000,
  )

proc jobError(taskName = ""): string =
  for job in listJobs():
    if job.lastError.len > 0 and (taskName.len == 0 or job.taskName == taskName):
      return job.lastError

suite "checkpoints":
  setup:
    resetCounters()

  teardown:
    closeQueueDatabases()

  test "checkpoint outside a job fails":
    expect CheckpointError:
      checkpointOutside()

  test "retry resumes after the last completed checkpoint":
    for kind in [bkSqlite, bkMemory]:
      resetCounters()
      crashBeforeWrite = true
      let dbPath = openBackend(kind)
      defer: teardownQuee(dbPath)

      discard importBatch.enqueue("batch-1").run()
      check processOne()
      check loadRuns == 1
      check writeRuns == 0
      check seenCount == 7
      check listJobs().len == 1
      check listJobs()[0].attempts == 1
      check lastCheckpointName(listJobs()[0].payload) == "load"
      let jobsHtml = renderJobs("/quee", "", "")
      check "Checkpoint" in jobsHtml
      check ">load<" in jobsHtml

      check processOne()
      check loadRuns == 1
      check writeRuns == 1
      check seenCount == 7
      check listJobs().len == 0

  test "in-progress checkpoint runs again":
    for kind in [bkSqlite, bkMemory]:
      resetCounters()
      let dbPath = openBackend(kind)
      defer: teardownQuee(dbPath)

      discard partialStep.enqueue().run()
      check processOne()
      check partialRuns == 1
      check afterPartialRuns == 0
      check lastCheckpointName(listJobs()[0].payload) == ""

      check processOne()
      check partialRuns == 2
      check afterPartialRuns == 1
      check listJobs().len == 0

  test "void and value checkpoints replay":
    for kind in [bkSqlite, bkMemory]:
      resetCounters()
      failValues = true
      let dbPath = openBackend(kind)
      defer: teardownQuee(dbPath)

      discard voidAndValue.enqueue().run()
      check processOne()
      check voidRuns == 1
      check valueRuns == 1
      check seenLabel == "ok"
      check lastCheckpointName(listJobs()[0].payload) == "label"

      check processOne()
      check voidRuns == 1
      check valueRuns == 1
      check seenLabel == "ok"
      check listJobs().len == 0

  test "out-of-order checkpoint does not rerun completed steps":
    for kind in [bkSqlite, bkMemory]:
      resetCounters()
      let dbPath = openBackend(kind)
      defer: teardownQuee(dbPath)

      discard orderedSteps.enqueue().run()
      check processOne()
      check orderARuns == 1
      check orderBRuns == 1
      check processOne()
      check orderARuns == 1
      check orderBRuns == 1
      check "expected checkpoint 'a'" in jobError()

  test "recurring job starts the next run with no checkpoints":
    for kind in [bkSqlite, bkMemory]:
      resetCounters()
      let dbPath = openBackend(kind)
      defer: teardownQuee(dbPath)

      discard recurTick.enqueue().every(100.milliseconds)
      sleep(150)
      check processOne()
      check recurRuns == 1
      sleep(150)
      check processOne()
      check recurRuns == 2

  test "nested and duplicate checkpoints fail":
    for kind in [bkSqlite, bkMemory]:
      let dbPath = openBackend(kind)
      defer: teardownQuee(dbPath)

      setMaxAttempts(1)
      discard nestedSteps.enqueue().run()
      check processOne()
      check "checkpoints cannot be nested" in jobError("nestedSteps")
      check lastCheckpointName(listJobs()[0].payload) == ""

      discard duplicateStep.enqueue().run()
      check processOne()
      check "duplicate checkpoint 'same'" in jobError("duplicateStep")

  test "saveCheckpoint rejects a stale lease":
    for kind in [bkSqlite, bkMemory]:
      let dbPath = openBackend(kind)
      defer: teardownQuee(dbPath)

      let job = leaseProbe.enqueue().run()
      var claimed: ClaimedJob
      withQueeDbLock:
        claimed = currentBackend().claimDue("default", @[], jobLeaseTimeout())
      check claimed.id == job.id
      check claimed.leaseId.len > 0

      var before = 0'i64
      for snap in listJobs():
        if snap.id == job.id:
          before = snap.leasedUntilMs
      sleep(30)

      withQueeDbLock:
        check not currentBackend().saveCheckpoint(
          "default", claimed.id, "stale-lease", "step", newJInt(1), jobLeaseTimeout(),
        )
      for snap in listJobs():
        if snap.id == job.id:
          check snap.leasedUntilMs == before
          check lastCheckpointName(snap.payload) == ""

      withQueeDbLock:
        check currentBackend().saveCheckpoint(
          "default", claimed.id, claimed.leaseId, "step", newJInt(1), jobLeaseTimeout(),
        )
      var found = false
      for snap in listJobs():
        if snap.id == job.id:
          found = true
          check lastCheckpointName(snap.payload) == "step"
          check snap.leasedUntilMs > before
          check snap.payload["checkpoints"][0]["result"].getInt() == 1
      check found

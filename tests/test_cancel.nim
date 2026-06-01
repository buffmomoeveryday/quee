import std/[locks, os, times, unittest]
import quee
import helpers

var hits = 0
var started = false
var stopped = false
var finished = false
var stateLock: Lock
initLock(stateLock)

proc setFlag(flag: var bool; value: bool) =
  withLock stateLock:
    flag = value

proc getFlag(flag: var bool): bool =
  withLock stateLock:
    result = flag

task countCancelHit():
  inc hits

task cancellableWork():
  setFlag(started, true)
  for _ in 0 ..< 50:
    if cancellationRequested():
      setFlag(stopped, true)
      return
    sleep(10)
  setFlag(finished, true)

proc waitUntil(check: proc(): bool; timeoutMs: int): bool =
  let deadline = epochTime() + (timeoutMs.float / 1000.0)
  while epochTime() < deadline:
    if check():
      return true
    sleep(10)
  false

suite "job cancellation":
  var dbPath: string

  setup:
    hits = 0
    setFlag(started, false)
    setFlag(stopped, false)
    setFlag(finished, false)
    dbPath = setupQuee()

  teardown:
    teardownQuee(dbPath)

  test "cancel queued job by builder":
    let job = countCancelHit.enqueue().run()
    check job.cancel()
    check not processOne()
    check hits == 0

  test "cancel scheduled job by id":
    let job = countCancelHit.enqueue().after(1.hours)
    check cancelJob(job.id)
    sleep(20)
    check not processOne()
    check hits == 0

  test "running job can stop cooperatively":
    let job = cancellableWork.enqueue().run()
    startQuee(pollIntervalMs = 10, concurrency = 1)
    check waitUntil(proc (): bool = getFlag(started), 500)
    check job.cancel()
    check waitUntil(proc (): bool = getFlag(stopped), 1000)
    check not getFlag(finished)

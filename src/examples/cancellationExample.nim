import std/[locks, os, times]
import ../quee

let dbPath = getTempDir() / "quee_example_cancellation"
if dirExists(dbPath):
  removeDir(dbPath)

var stopped = false
var doneLock: Lock
initLock(doneLock)

proc markStopped() =
  withLock doneLock:
    stopped = true

proc hasStopped(): bool =
  withLock doneLock:
    result = stopped

initQuee(dbPath, pollIntervalMs = 10, workerConcurrency = 1)

task queuedOnly():
  echo "this should not run"

task longRunning():
  for step in 1 .. 100:
    if cancellationRequested():
      echo "long-running job cancelled at step ", step
      markStopped()
      return
    sleep(25)
  echo "long-running job finished normally"

let queued = queuedOnly.enqueue().after(1.hours)
echo "queued cancel result: ", queued.cancel()

let running = longRunning.enqueue().run()
startQuee(pollIntervalMs = 10, concurrency = 1)

sleep(100)
echo "running cancel result: ", running.cancel()

while not hasStopped():
  sleep(10)

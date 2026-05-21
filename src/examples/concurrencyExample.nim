import std/[locks, os]
import ../quee

let dbPath = getTempDir() / "quee_example_concurrency"
if dirExists(dbPath):
  removeDir(dbPath)

var done = 0
var doneLock: Lock
initLock(doneLock)

proc markDone() =
  withLock doneLock:
    inc done

proc doneCount(): int =
  withLock doneLock:
    result = done

initQuee(dbPath, pollIntervalMs = 10, workerConcurrency = 4)

task slowExternalCall(id: int):
  echo "start slow job ", id
  sleep(500)
  echo "done slow job ", id
  markDone()

for id in 1 .. 8:
  discard slowExternalCall.enqueue(id).run()

startQuee()

while doneCount() < 8:
  sleep(20)

echo "All slow jobs finished"

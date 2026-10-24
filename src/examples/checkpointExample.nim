import std/os
import ../quee

let dbPath = getTempDir() / "quee_example_checkpoint"
if dirExists(dbPath):
  removeDir(dbPath)

initQuee(dbPath, retryDelayMs = 0, maxAttempts = 3)

var loaded = 0
var written = 0
var crashesLeft = 1

task importBatch(batchId: string):
  let count = checkpoint("load"):
    inc loaded
    echo "loading ", batchId
    2

  if crashesLeft > 0:
    dec crashesLeft
    echo "crash after load; rows=", count
    raise newException(ValueError, "crash after load")

  checkpoint("write"):
    inc written
    echo "writing ", count, " rows for ", batchId

discard importBatch.enqueue("batch-1").run()
echo "first attempt"
discard processOne()
echo "resume"
discard processOne()
echo "loaded=", loaded, " written=", written

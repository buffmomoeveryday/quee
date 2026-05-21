import std/[os]
import ../quee

let dbPath = getTempDir() / "quee_example_worker_config"
if dirExists(dbPath):
  removeDir(dbPath)

initQuee(dbPath, pollIntervalMs = 200, workerConcurrency = 1)

echo "Initial poll interval: ", pollInterval(), "ms"
echo "Initial concurrency: ", workerConcurrency()

setPollInterval(50)
setWorkerConcurrency(2)

echo "Updated poll interval: ", pollInterval(), "ms"
echo "Updated concurrency: ", workerConcurrency()

task showConfig():
  echo "worker picked up configured job"

discard showConfig.enqueue().run()
discard processOne()

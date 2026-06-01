import std/[os, times]
import ../quee

let dbPath = getTempDir() / "quee_example_deploy_update"
if dirExists(dbPath):
  removeDir(dbPath)

initQuee(dbPath, pollIntervalMs = 10)

var hits = 0

task onceAfterDeploy():
  inc hits
  echo "this stale one-shot job should not run after update"

task recurringAfterDeploy():
  inc hits
  echo "recurring job resumed on its next interval"

discard onceAfterDeploy.enqueue().run()
discard recurringAfterDeploy.enqueue().every(100.milliseconds)

sleep(150)

# Simulate a new app version starting after downtime. One-shot jobs that became
# due while the worker was stopped are discarded. Recurring jobs are advanced to
# their next run instead of catching up immediately.
initQuee(dbPath, pollIntervalMs = 10, skipMissedJobs = true)

echo "No stale job should run immediately"
discard processOne()
echo "hits after skipped missed jobs: ", hits

sleep(150)
discard processOne()
echo "hits after recurring resumes: ", hits

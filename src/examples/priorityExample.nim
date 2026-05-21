import std/[os, strformat]
import ../quee

let dbPath = getTempDir() / "quee_example_priority"
if dirExists(dbPath):
  removeDir(dbPath)

initQuee(dbPath, pollIntervalMs = 10)

task normalJob(name: string):
  priority 5
  echo &"normal priority: {name}"

task urgentJob(name: string):
  priority 1
  echo &"urgent priority: {name}"

discard normalJob.enqueue("first enqueued").run()
discard urgentJob.enqueue("second enqueued").run()
discard normalJob.enqueue("runtime override").priority(2).run()

echo "Due jobs run by priority, not enqueue order"
while processOne():
  discard

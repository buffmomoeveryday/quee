import std/[os, strformat]
import ../quee

let dbPath = getTempDir() / "quee_example_queues"
if dirExists(dbPath):
  removeDir(dbPath)

initQuee(dbPath, queues = ["default", "emails", "reports"], pollIntervalMs = 10)

task sendEmail(email: string):
  queue "emails"
  echo &"email queue: {email}"

task buildReport(userId: string):
  queue "reports"
  echo &"reports queue: {userId}"

task genericJob(name: string):
  echo &"default queue: {name}"

discard genericJob.enqueue("default work").run()
discard sendEmail.enqueue("ada@example.com").run()
discard buildReport.enqueue("user-42").run()
discard sendEmail.enqueue("override to default", queue = "default").run()

printRegisteredTasks()

echo "Draining all configured queues"
while processOne():
  discard

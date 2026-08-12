import std/unittest
import quee
import helpers

var hits = 0

task onDefault():
  inc hits

task onEmails():
  queue "emails"
  inc hits

suite "multi-queue":
  var dbPath: string

  setup:
    hits = 0
    dbPath = setupQuee(queues = ["default", "emails", "urgent"])

  teardown:
    teardownQuee(dbPath)

  test "enqueue targets default queue":
    let job = onDefault.enqueue().run()
    check cancelJob(job.id, queue = "default")
    check not processOne()

  test "task using clause picks queue":
    let job = onEmails.enqueue().run()
    check not cancelJob(job.id, queue = "default")
    check cancelJob(job.id, queue = "emails")
    check not processOne()

  test "runtime queue= overrides task default":
    let job = onEmails.enqueue(queue = "urgent").run()
    check not cancelJob(job.id, queue = "emails")
    check cancelJob(job.id, queue = "urgent")
    check not processOne()

  test "unknown queue raises":
    expect ValueError:
      discard onDefault.enqueue(queue = "missing").run()

  test "listTasks sees registered tasks":
    let tasks = listTasks()
    check tasks.len >= 2
    check tasks[0].name == "onDefault"
    check tasks[1].name == "onEmails"
    check tasks[1].defaultQueue == "emails"

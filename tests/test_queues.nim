import std/[os, unittest]
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

  test "enqueue targets queue subdirectory":
    discard onDefault.enqueue().run()
    check dirExists(dbPath / "default")
    check processOne()
    check hits == 1

  test "task using clause picks queue":
    hits = 0
    discard onEmails.enqueue().run()
    check dirExists(dbPath / "emails")
    check processOne()
    check hits == 1

  test "runtime queue= overrides task default":
    hits = 0
    discard onEmails.enqueue(queue = "urgent").run()
    check dirExists(dbPath / "urgent")
    hits = 0
    check processOne()
    check hits == 1

  test "unknown queue raises":
    expect ValueError:
      discard onDefault.enqueue(queue = "missing").run()

  test "listTasks sees registered tasks":
    let tasks = listTasks()
    check tasks.len >= 2
    check tasks[0].name == "onDefault"
    check tasks[1].name == "onEmails"
    check tasks[1].defaultQueue == "emails"

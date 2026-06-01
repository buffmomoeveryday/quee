import std/[os, times, unittest]
import quee
import helpers

var hitCount = 0

task bumpCounter():
  inc hitCount

task echoArgs(email: string, subject: string):
  if email == "a@b.com" and subject == "Hi":
    inc hitCount

suite "enqueue and run":
  var dbPath: string

  setup:
    hitCount = 0
    dbPath = setupQuee()

  teardown:
    teardownQuee(dbPath)

  test "run executes via processOne":
    discard bumpCounter.enqueue().run()
    check processOne()
    check hitCount == 1
    check not processOne()

  test "run passes task arguments":
    discard echoArgs.enqueue("a@b.com", "Hi").run()
    check processOne()
    check hitCount == 1

  test "after delays execution":
    hitCount = 0
    discard bumpCounter.enqueue().after(200.milliseconds)
    check not processOne()
    sleep(250)
    check processOne()
    check hitCount == 1

  test "every interval requeues":
    hitCount = 0
    discard bumpCounter.enqueue().every(100.milliseconds)
    sleep(150)
    check processOne()
    check hitCount == 1
    sleep(150)
    check processOne()
    check hitCount == 2

  test "initQuee skipMissedJobs discards due one-shot jobs":
    hitCount = 0
    discard bumpCounter.enqueue().run()
    initQuee(dbPath, skipMissedJobs = true)
    check not processOne()
    check hitCount == 0

  test "initQuee skipMissedJobs advances recurring jobs":
    hitCount = 0
    discard bumpCounter.enqueue().every(100.milliseconds)
    sleep(150)
    initQuee(dbPath, skipMissedJobs = true)
    check not processOne()
    check hitCount == 0
    sleep(150)
    check processOne()
    check hitCount == 1

suite "startQuee worker":
  test "background worker runs jobs":
    hitCount = 0
    let path = setupQuee()
    defer: teardownQuee(path)

    discard bumpCounter.enqueue().run()
    startQuee(50)
    sleep(300)
    check hitCount >= 1

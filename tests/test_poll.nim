import std/[os, unittest]
import quee
import helpers

var hits = 0
var workerDbPath = ""

task tick():
  inc hits

suite "poll interval (config only)":
  test "initQuee sets default poll interval":
    let path = setupQuee()
    defer: teardownQuee(path)
    check pollInterval() == DefaultPollIntervalMs

  test "initQuee pollIntervalMs parameter":
    let base = uniqueTestDb()
    initQuee(base, pollIntervalMs = 75)
    check pollInterval() == 75
    teardownQuee(base)

  test "invalid poll interval raises":
    expect ValueError:
      setPollInterval(0)

suite "poll interval (worker)":
  setup:
    if workerDbPath.len == 0:
      workerDbPath = setupQuee()

  test "startQuee overrides poll interval":
    check not workersAreRunning()
    setPollInterval(1000)
    startQuee(40)
    check pollInterval() == 40

  test "setPollInterval before startQuee":
    hits = 0
    setPollInterval(30)
    discard tick.enqueue().run()
    sleep(200)
    check hits >= 1

  test "setPollInterval while worker is running":
    hits = 0
    setPollInterval(25)
    discard tick.enqueue().run()
    sleep(150)
    check hits >= 1

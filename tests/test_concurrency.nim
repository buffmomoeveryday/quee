import std/[os, times, unittest]
import quee
import helpers

var hits = 0

task work():
  sleep(150)
  inc hits

suite "worker concurrency":
  test "default concurrency is 1":
    check not workersAreRunning()
    let path = setupQuee()
    defer: teardownQuee(path)
    check workerConcurrency() == DefaultWorkerConcurrency

  test "initQuee workerConcurrency parameter":
    check not workersAreRunning()
    let base = uniqueTestDb()
    initQuee(base, workerConcurrency = 4)
    check workerConcurrency() == 4
    teardownQuee(base)

  test "invalid concurrency raises":
    expect ValueError:
      setWorkerConcurrency(0)
    expect ValueError:
      setWorkerConcurrency(65)

  test "concurrency runs due jobs in parallel":
    check not workersAreRunning()
    hits = 0
    let path = setupQuee()
    defer: teardownQuee(path)
    discard work.enqueue().run()
    discard work.enqueue().run()
    let t0 = epochTime()
    startQuee(concurrency = 2, pollIntervalMs = 10)
    while hits < 2 and epochTime() - t0 < 3.0:
      sleep(20)
    check hits == 2
    check epochTime() - t0 < 0.55

  test "double startQuee raises":
    check workersAreRunning()
    expect ValueError:
      startQuee()

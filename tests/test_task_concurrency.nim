import std/[locks, os, times, unittest]
import quee
import helpers

var
  active = 0
  hits = 0
  maxActive = 0
  taskLock: Lock

initLock(taskLock)

proc resetCounters() =
  acquire(taskLock)
  try:
    active = 0
    hits = 0
    maxActive = 0
  finally:
    release(taskLock)

proc snapshot(): tuple[hits: int, maxActive: int] =
  acquire(taskLock)
  try:
    result = (hits, maxActive)
  finally:
    release(taskLock)

task limitedWork():
  concurrency 2

  acquire(taskLock)
  try:
    inc active
    if active > maxActive:
      maxActive = active
  finally:
    release(taskLock)

  sleep(150)

  acquire(taskLock)
  try:
    dec active
    inc hits
  finally:
    release(taskLock)

suite "task concurrency":
  test "task concurrency limits simultaneous handlers":
    check not workersAreRunning()
    resetCounters()
    let path = setupQuee()
    defer: teardownQuee(path)

    for _ in 0 ..< 4:
      discard limitedWork.enqueue().run()

    let t0 = epochTime()
    startQuee(concurrency = 4, pollIntervalMs = 10)
    while snapshot().hits < 4 and epochTime() - t0 < 3.0:
      sleep(20)

    let result = snapshot()
    check result.hits == 4
    check result.maxActive == 2

  test "listTasks includes task concurrency":
    let tasks = listTasks()
    var found = false
    for info in tasks:
      if info.name == "limitedWork":
        found = true
        check info.taskConcurrency == 2
    check found

import std/unittest
import quee
import helpers
import external_tasks

suite "imported tasks":
  test "task defined in another module can be enqueued":
    externalHits = 0
    let path = setupQuee()
    defer: teardownQuee(path)

    discard externalTask.enqueue("imported").run()
    check processOne()
    check externalHits == 1

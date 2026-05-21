import std/[sequtils, times, unittest]
import quee
import helpers

var order: seq[int] = @[]

task lowPrio():
  order.add(10)

task highPrio():
  priority 1
  order.add(1)

task midPrio():
  priority 5
  order.add(5)

suite "priority":
  var dbPath: string

  setup:
    order = @[]
    dbPath = setupQuee()

  teardown:
    teardownQuee(dbPath)

  test "worker runs lower number first among due jobs":
    discard lowPrio.enqueue().run()
    discard highPrio.enqueue().run()
    check processOne()
    check order == @[1]
    check processOne()
    check order == @[1, 10]

  test "enqueue priority= overrides task default":
    order = @[]
    discard midPrio.enqueue(priority = 2).run()
    discard lowPrio.enqueue(priority = 9).run()
    check processOne()
    check order == @[5]
    order = @[]
    check processOne()
    check order == @[10]

  test "priority builder chain overrides":
    order = @[]
    discard lowPrio.enqueue().priority(1).run()
    discard highPrio.enqueue().priority(10).run()
    check processOne()
    check order == @[10]

  test "future high priority does not block due lower priority":
    order = @[]
    discard highPrio.enqueue().after(1.hours)
    discard lowPrio.enqueue().run()
    check processOne()
    check order == @[10]

  test "invalid priority at enqueue raises":
    expect ValueError:
      discard lowPrio.enqueue(priority = 0).run()
    expect ValueError:
      discard lowPrio.enqueue(priority = 11).run()

  test "listTasks includes default priority":
    let tasks = listTasks()
    let hi = tasks.filterIt(it.name == "highPrio")[0]
    check hi.defaultPriority == 1
    let lo = tasks.filterIt(it.name == "lowPrio")[0]
    check lo.defaultPriority == 5

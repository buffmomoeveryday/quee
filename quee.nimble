# Package

version       = "0.1.0"
author        = "Siddhartha Khanal"
description   = "Lightweight background job queue for Nim with fluent scheduling"
license       = "MIT"
srcDir        = "src"


# Dependencies

requires "nim >= 2.0.8"

requires "malebolgia >= 0.1.0"
requires "limdb"
requires "mummy"

# Tasks
task test, "Run the test suite":
  exec "nim c -r --threads:on --mm:arc --path:src tests/test_schedule.nim"
  exec "nim c -r --threads:on --mm:arc --path:src tests/test_queue.nim"
  exec "nim c -r --threads:on --mm:arc --path:src tests/test_queues.nim"
  exec "nim c -r --threads:on --mm:arc --path:src tests/test_priority.nim"
  exec "nim c -r --threads:on --mm:arc --path:src tests/test_poll.nim"
  exec "nim c -r --threads:on --mm:arc --path:src tests/test_concurrency.nim"

task bench, "Run benchmarks":
  exec "nim c --threads:on --mm:arc --path:src -o:benchmarks/bench_quee benchmarks/bench_quee.nim"
  exec "./benchmarks/bench_quee"

task buildScheduler, "Build the scheduler example":
  exec "nim c --threads:on --mm:arc --path:src src/examples/exampleScheduler.nim"

task buildServer, "Build the mummy web server example":
  exec "nim c --threads:on --mm:arc --path:src src/examples/mummyWebServerExample.nim"

task buildExamples, "Build all examples":
  exec "nim c --threads:on --mm:arc --path:src src/examples/basicUsage.nim"
  exec "nim c --threads:on --mm:arc --path:src src/examples/priorityExample.nim"
  exec "nim c --threads:on --mm:arc --path:src src/examples/queuesExample.nim"
  exec "nim c --threads:on --mm:arc --path:src src/examples/workerConfigExample.nim"
  exec "nim c --threads:on --mm:arc --path:src src/examples/deployUpdateExample.nim"
  exec "nim c --threads:on --mm:arc --path:src src/examples/concurrencyExample.nim"
  exec "nim c --threads:on --mm:arc --path:src src/examples/exampleScheduler.nim"
  exec "nim c --threads:on --mm:arc --path:src src/examples/mummyWebServerExample.nim"

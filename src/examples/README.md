# Quee Examples

Compile examples from the project root:

```sh
nim c --threads:on --mm:arc --path:src src/examples/basicUsage.nim
```

Or build all examples:

```sh
nimble buildExamples
```

## Files

- `basicUsage.nim` - define tasks, enqueue immediate work, enqueue delayed work, and drain with `processOne()`.
- `priorityExample.nim` - task default priority, runtime priority override, and priority execution order.
- `queuesExample.nim` - multiple queues, task queue defaults, runtime queue override, and registered task listing.
- `workerConfigExample.nim` - poll interval and worker concurrency configuration without starting a long-running worker.
- `deployUpdateExample.nim` - skip missed jobs on app restart/deploy with `skipMissedJobs = true`.
- `cancellationExample.nim` - cancel queued jobs and cooperatively stop a running job.
- `backendExample.nim` - switch between LIMDB, SQLite, and a custom in-memory backend.
- `concurrencyExample.nim` - background worker concurrency with slow simulated external work.
- `exampleScheduler.nim` - long-running SQLite-backed scheduler with interval, delayed, daily, weekly, queue, priority, poll interval, and concurrency examples.
- `mummyWebServerExample.nim` - enqueue background jobs from a Mummy HTTP request handler.

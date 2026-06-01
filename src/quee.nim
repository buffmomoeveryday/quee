## Quee — background job queue with fluent scheduling.
##
## See the project `README.md` for usage. Minimal setup::
##
##   import quee
##   initQuee("./mydb")
##   task myJob(x: string): echo x
##   discard myJob.enqueue("hi").run()
##   startQuee()

import quee/[
  types,
  backend,
  limdbbackend,
  sqlitebackend,
  registry,
  schedule,
  builder,
  worker,
  taskmacro,
  tasksyntax,
  log,
  priority,
  jobkey,
]

export types
export backend
export limdbbackend
export sqlitebackend
export registry
export closeQueueDatabases
export DefaultPollIntervalMs
export DefaultWorkerConcurrency
export MaxWorkerConcurrency
export setPollInterval
export pollInterval
export setWorkerConcurrency
export workerConcurrency
export workersAreRunning
export resetQueeRegistry
export listTasks
export printRegisteredTasks
export schedule
export builder
export worker
export taskmacro
export tasksyntax
export log
export priority
export jobkey

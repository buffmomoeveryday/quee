import std/[json, tables]

type
  TaskHandler* = proc(args: JsonNode)

  TaskInfo* = object
    name*: string
    defaultQueue*: string
    defaultPriority*: int
    taskConcurrency*: int

  ScheduleKind* = enum
    skOnce
    skCron
    skEveryDay
    skEveryWeek
    skEveryInterval

  JobSchedule* = object
    kind*: ScheduleKind
    runAt*: float
    cronExpr*: string
    weekday*: int
    atHour*: int
    atMinute*: int
    atSecond*: int
    intervalSecs*: float

  JobBuilder* = object
    taskName*: string
    args*: JsonNode
    jobId*: string
    dbPath*: string
    queueName*: string
    priority*: int
    schedule*: JobSchedule

  RunBuilder* = JobBuilder
  EveryBuilder* = JobBuilder
  CronBuilder* = JobBuilder

  DayBuilder* = object
    base*: EveryBuilder

  WeekdayBuilder* = object
    base*: EveryBuilder
    weekday*: int

  QueeRegistry* = object
    handlers*: Table[string, TaskHandler]
    tasks*: Table[string, TaskInfo]
    taskOrder*: seq[string]
    basePath*: string
    queues*: seq[string]
    queuePaths*: Table[string, string]
    defaultQueue*: string
    pollIntervalMs*: int
    workerConcurrency*: int
    jobLeaseTimeoutMs*: int

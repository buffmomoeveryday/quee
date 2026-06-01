import std/[json, random, strformat, times]
import ./[types, backend, registry, schedule, priority]

proc newJobId(taskName: string): string =
  &"{epochTime()}_{taskName}_{rand(100_000)}"

proc intervalToSecs(interval: TimeInterval): float =
  let t0 = now()
  (t0 + interval).toTime().toUnixFloat() - t0.toTime().toUnixFloat()

proc newJobBuilder*(
  taskName: string; args: JsonNode; queue: string; priority: int
): JobBuilder =
  JobBuilder(
    taskName: taskName,
    args: args,
    jobId: newJobId(taskName),
    dbPath: dbPath(queue),
    queueName: queue,
    priority: priority,
  )

proc priority*(b: JobBuilder, p: int): JobBuilder =
  ## Override job priority (1 = highest, 10 = lowest). Chain before ``.run()``, etc.
  validatePriority(p)
  result = b
  result.priority = p

proc id*(b: JobBuilder): string =
  ## Stable job id generated when the builder was created.
  b.jobId

proc cancel*(b: JobBuilder): bool =
  ## Cancel this job if it is still queued, or request cooperative cancellation
  ## if it is already running.
  cancelJob(b.jobId, b.queueName)

proc persistJob*(b: JobBuilder, schedule: JobSchedule): JobBuilder =
  var sched = schedule
  sched.runAt = computeInitialRunAt(sched)

  let jobData = %*{
    "id": b.jobId,
    "taskName": b.taskName,
    "queue": b.queueName,
    "priority": b.priority,
    "args": b.args,
    "runAt": sched.runAt,
    "schedule": scheduleToJson(sched),
  }

  withQueeDbLock:
    currentBackend().enqueue(b.queueName, jobData)

  result = b
  result.schedule = sched

proc run*(b: JobBuilder): JobBuilder =
  ## Enqueue and run as soon as the worker picks it up.
  b.persistJob(JobSchedule(kind: skOnce, runAt: 0.0))

proc after*(b: JobBuilder, delay: TimeInterval): JobBuilder =
  ## Enqueue a one-shot job to run after `delay`.
  b.persistJob(JobSchedule(kind: skOnce, runAt: (now() + delay).toTime().toUnixFloat()))

proc at*(b: JobBuilder, time: DateTime): JobBuilder =
  ## Enqueue a one-shot job to run at `time`.
  b.persistJob(JobSchedule(kind: skOnce, runAt: time.toTime().toUnixFloat()))

proc schedule*(b: JobBuilder): RunBuilder =
  ## Start a scheduling chain (cron, calendar every).
  RunBuilder(b)

proc cron*(b: RunBuilder): CronBuilder =
  var c = CronBuilder(b)
  c.schedule = JobSchedule(kind: skCron)
  result = c

proc expr*(b: CronBuilder, expression: string): JobBuilder =
  var sched = b.schedule
  sched.cronExpr = expression
  JobBuilder(b).persistJob(sched)

proc every*(b: RunBuilder): EveryBuilder =
  var e = EveryBuilder(b)
  e.schedule = JobSchedule()
  e

proc every*(b: JobBuilder, interval: TimeInterval): JobBuilder =
  ## Enqueue a recurring job every `interval` (e.g. `1.seconds`).
  let secs = intervalToSecs(interval)
  if secs <= 0:
    raise newException(ValueError, "interval must be positive")
  b.persistJob(JobSchedule(kind: skEveryInterval, intervalSecs: secs))

proc every*(b: JobBuilder, interval: Duration): JobBuilder =
  ## Enqueue a recurring job every `interval` (Duration form).
  let secs = interval.inSeconds.float
  if secs <= 0:
    raise newException(ValueError, "interval must be positive")
  b.persistJob(JobSchedule(kind: skEveryInterval, intervalSecs: secs))

proc day*(b: EveryBuilder): DayBuilder =
  var e = b
  e.schedule.kind = skEveryDay
  DayBuilder(base: e)

proc week*(b: EveryBuilder): WeekdayBuilder =
  var e = b
  e.schedule.kind = skEveryWeek
  WeekdayBuilder(base: e, weekday: now().weekday.int)

proc monday*(b: EveryBuilder): WeekdayBuilder =
  var e = b; e.schedule.kind = skEveryWeek; WeekdayBuilder(base: e, weekday: 1)
proc tuesday*(b: EveryBuilder): WeekdayBuilder =
  var e = b; e.schedule.kind = skEveryWeek; WeekdayBuilder(base: e, weekday: 2)
proc wednesday*(b: EveryBuilder): WeekdayBuilder =
  var e = b; e.schedule.kind = skEveryWeek; WeekdayBuilder(base: e, weekday: 3)
proc thursday*(b: EveryBuilder): WeekdayBuilder =
  var e = b; e.schedule.kind = skEveryWeek; WeekdayBuilder(base: e, weekday: 4)
proc friday*(b: EveryBuilder): WeekdayBuilder =
  var e = b; e.schedule.kind = skEveryWeek; WeekdayBuilder(base: e, weekday: 5)
proc saturday*(b: EveryBuilder): WeekdayBuilder =
  var e = b; e.schedule.kind = skEveryWeek; WeekdayBuilder(base: e, weekday: 6)
proc sunday*(b: EveryBuilder): WeekdayBuilder =
  var e = b; e.schedule.kind = skEveryWeek; WeekdayBuilder(base: e, weekday: 7)

proc at*(d: DayBuilder, time: DateTime): JobBuilder =
  var sched = d.base.schedule
  sched.kind = skEveryDay
  sched.atHour = time.hour
  sched.atMinute = time.minute
  sched.atSecond = time.second
  JobBuilder(d.base).persistJob(sched)

proc at*(d: DayBuilder): JobBuilder =
  d.at(withTime(now(), 0, 0, 0))

proc at*(w: WeekdayBuilder, time: DateTime): JobBuilder =
  var sched = w.base.schedule
  sched.kind = skEveryWeek
  sched.weekday = w.weekday
  sched.atHour = time.hour
  sched.atMinute = time.minute
  sched.atSecond = time.second
  JobBuilder(w.base).persistJob(sched)

proc at*(w: WeekdayBuilder): JobBuilder =
  w.at(withTime(now(), 0, 0, 0))

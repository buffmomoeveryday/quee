import std/[json, strformat, times, unittest]
import quee

suite "schedule serialization":
  test "roundtrip schedule json":
    let original = JobSchedule(
      kind: skEveryInterval,
      runAt: 123.0,
      intervalSecs: 5.0,
      weekday: 3,
      atHour: 9,
      atMinute: 30,
      atSecond: 0,
      cronExpr: "0 9 * * *",
    )
    let restored = scheduleFromJson(scheduleToJson(original))
    check restored.kind == original.kind
    check restored.intervalSecs == original.intervalSecs
    check restored.cronExpr == original.cronExpr
    check restored.atHour == original.atHour

suite "cron matching":
  test "matches current time":
    let dt = now()
    let expr = &"{dt.minute} {dt.hour} * * *"
    check cronMatches(expr, dt)

  test "rejects wrong minute":
    let dt = now()
    let wrongMinute = if dt.minute == 0: 1 else: dt.minute - 1
    let expr = &"{wrongMinute} {dt.hour} * * *"
    check not cronMatches(expr, dt)

  test "step syntax":
    let dt = dateTime(2026, mJan, 1, 10, 10, 0)
    check cronMatches("*/10 * * * *", dt)
    check not cronMatches("*/10 * * * *", dt + 5.minutes)

suite "isJobDue":
  test "once job due when runAt is zero":
    let job = $(%*{
      "runAt": 0.0,
      "schedule": scheduleToJson(JobSchedule(kind: skOnce, runAt: 0.0)),
    })
    check isJobDue(job)

  test "once job not due in the future":
    let future = (now() + 1.hours).toTime().toUnixFloat()
    let job = $(%*{
      "runAt": future,
      "schedule": scheduleToJson(JobSchedule(kind: skOnce, runAt: future)),
    })
    check not isJobDue(job)

  test "legacy job without schedule field":
    let job = $(%*{"runAt": 0.0})
    check isJobDue(job)

suite "computeInitialRunAt":
  test "interval schedule runs soon":
    let runAt = computeInitialRunAt(JobSchedule(kind: skEveryInterval, intervalSecs: 60.0))
    check runAt >= epochTime()
    check runAt <= epochTime() + 61.0

  test "cron schedule advances to a future minute":
    let runAt = computeInitialRunAt(JobSchedule(kind: skCron, cronExpr: "*/1 * * * *"))
    check runAt > epochTime()
    check runAt <= epochTime() + 61.0

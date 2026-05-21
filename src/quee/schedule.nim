import std/[json, strutils, times]
import ./types

proc scheduleToJson*(s: JobSchedule): JsonNode =
  %*{
    "kind": $s.kind,
    "runAt": s.runAt,
    "cronExpr": s.cronExpr,
    "weekday": s.weekday,
    "atHour": s.atHour,
    "atMinute": s.atMinute,
    "atSecond": s.atSecond,
    "intervalSecs": s.intervalSecs,
  }

proc scheduleFromJson*(j: JsonNode): JobSchedule =
  JobSchedule(
    kind: parseEnum[ScheduleKind](j["kind"].getStr(), skOnce),
    runAt: j["runAt"].getFloat(),
    cronExpr: j["cronExpr"].getStr(""),
    weekday: j["weekday"].getInt(-1),
    atHour: j["atHour"].getInt(0),
    atMinute: j["atMinute"].getInt(0),
    atSecond: j["atSecond"].getInt(0),
    intervalSecs: j["intervalSecs"].getFloat(0),
  )

proc withTime*(dt: DateTime, hour, minute, second: int): DateTime =
  dateTime(dt.year, dt.month, dt.monthday, hour, minute, second)

proc nextWeeklyRun*(weekday, hour, minute, second: int): DateTime =
  var candidate = withTime(now(), hour, minute, second)
  while candidate.weekday.int != weekday:
    candidate = candidate + 1.days
  if candidate <= now():
    candidate = candidate + 7.days
  candidate

proc nextDailyRun*(hour, minute, second: int): DateTime =
  var candidate = withTime(now(), hour, minute, second)
  if candidate <= now():
    candidate = candidate + 1.days
  candidate

proc computeInitialRunAt*(schedule: JobSchedule): float =
  case schedule.kind
  of skOnce:
    if schedule.runAt > 0: schedule.runAt else: 0.0
  of skCron:
    0.0
  of skEveryDay:
    nextDailyRun(schedule.atHour, schedule.atMinute, schedule.atSecond)
      .toTime().toUnixFloat()
  of skEveryWeek:
    nextWeeklyRun(schedule.weekday, schedule.atHour, schedule.atMinute, schedule.atSecond)
      .toTime().toUnixFloat()
  of skEveryInterval:
    epochTime() + schedule.intervalSecs

proc computeNextRunAt*(schedule: JobSchedule): float =
  case schedule.kind
  of skOnce:
    schedule.runAt
  of skCron:
    0.0
  of skEveryDay:
    nextDailyRun(schedule.atHour, schedule.atMinute, schedule.atSecond)
      .toTime().toUnixFloat()
  of skEveryWeek:
    nextWeeklyRun(schedule.weekday, schedule.atHour, schedule.atMinute, schedule.atSecond)
      .toTime().toUnixFloat()
  of skEveryInterval:
    epochTime() + schedule.intervalSecs

proc cronFieldMatches(field: string, value: int): bool =
  if field == "*":
    true
  elif field.startsWith("*/"):
    let step = parseInt(field[2..^1])
    step > 0 and value mod step == 0
  else:
    field == $value

proc cronMatches*(expr: string, dt: DateTime): bool =
  let parts = expr.splitWhitespace()
  if parts.len != 5:
    return false
  cronFieldMatches(parts[0], dt.minute) and
    cronFieldMatches(parts[1], dt.hour) and
    cronFieldMatches(parts[2], dt.monthday) and
    cronFieldMatches(parts[3], ord(dt.month)) and
    cronFieldMatches(parts[4], dt.weekday.int)

proc isJobDue*(rawJob: string): bool =
  let payload = parseJson(rawJob)
  let runAt = payload["runAt"].getFloat()
  if "schedule" notin payload:
    return runAt <= 0.0 or epochTime() >= runAt
  let schedule = scheduleFromJson(payload["schedule"])
  case schedule.kind
  of skCron:
    if runAt > 0 and epochTime() < runAt:
      false
    else:
      cronMatches(schedule.cronExpr, now())
  else:
    runAt <= 0.0 or epochTime() >= runAt

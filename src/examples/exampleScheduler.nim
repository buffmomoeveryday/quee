import std/[os, terminal, times]
import ../quee

let dbPath = "./mydb_scheduler"
if dirExists(dbPath):
  removeDir(dbPath)

initQuee(
  dbPath,
  queues = ["default", "fast", "slow", "critical"],
  pollIntervalMs = 50,
  workerConcurrency = 2,
  backendKind = bkSqlite,
)

task echoEverySecond():
  queue "fast"
  priority 5
  styledEcho fgGreen, "tick ", resetStyle, $now(), "\n"

task echoAfterDelay(message: string):
  queue "default"
  styledEcho fgYellow, "delayed ", resetStyle, message, " ", $now(), "\n"

task echoEveryTenSeconds():
  queue "slow"
  styledEcho fgBlue, "interval ", resetStyle, $now(), "\n"

task echoEveryDay():
  queue "slow"
  styledEcho fgMagenta, "daily ", resetStyle, $now(), "\n"

task echoEveryWeek():
  queue "slow"
  styledEcho fgCyan, "weekly ", resetStyle, $now(), "\n"

task echoEveryMinuteCron():
  queue "slow"
  styledEcho fgWhite, "cron ", resetStyle, $now(), "\n"

task urgentOnce(message: string):
  queue "critical"
  priority 1
  styledEcho fgRed, "urgent ", resetStyle, message, " ", $now(), "\n"

discard echoEverySecond.enqueue().every(1.seconds)
discard echoEveryTenSeconds.enqueue().every(initDuration(seconds = 10))
discard echoAfterDelay.enqueue("runs once after 3 seconds").after(3.seconds)
discard urgentOnce.enqueue("runs before other due work").run()

let todaySoon = now() + 30.seconds
discard echoEveryDay.enqueue().schedule().every().day().at(todaySoon)

let thisWeekSoon = now() + 45.seconds
discard echoEveryWeek.enqueue().schedule().every().week().at(thisWeekSoon)

discard echoEveryMinuteCron.enqueue().schedule().cron().expr("*/1 * * * *")

startQuee(pollIntervalMs = 50, concurrency = 2)
queeInfo("Scheduler running with queues default, fast, slow, critical (Ctrl+C to stop)...")
waitForQuee()

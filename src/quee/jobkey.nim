import std/[json, strformat, strutils]
import ./priority

const JobKeyPrefix* = "v2|"

proc runAtMillis(runAt: float): int64 =
  if runAt <= 0.0:
    0'i64
  else:
    int64(runAt * 1000.0)

proc jobStorageKey*(jobId: string; priority: int; runAt: float): string =
  validatePriority(priority)
  &"{JobKeyPrefix}p{priority:02d}|r{runAtMillis(runAt):020d}|{jobId}"

proc jobStorageKey*(payload: JsonNode): string =
  jobStorageKey(
    payload["id"].getStr(),
    jobPriority(payload),
    payload["runAt"].getFloat(0.0),
  )

proc isJobStorageKey*(key: string): bool =
  key.startsWith(JobKeyPrefix)

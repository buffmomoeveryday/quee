import std/[json, os]
import ./[backend, priority, schedule, types]

type
  MemoryJob = tuple[queue: string, payload: JsonNode]

  MemoryBackend* = ref object of QueueBackend
    basePath: string
    jobs: seq[MemoryJob]

proc newMemoryBackend*(): MemoryBackend =
  MemoryBackend(jobs: @[])

proc isBlockedTask(payload: JsonNode; blockedTasks: openArray[string]): bool =
  if blockedTasks.len == 0 or "taskName" notin payload:
    return false
  let taskName = payload["taskName"].getStr()
  for blocked in blockedTasks:
    if blocked == taskName:
      return true

method setup*(backend: MemoryBackend; basePath: string; queues: openArray[string]) {.gcsafe.} =
  {.cast(gcsafe).}:
    backend.basePath = basePath
    backend.jobs = @[]

method storagePath*(backend: MemoryBackend; queue: string): string {.gcsafe.} =
  {.cast(gcsafe).}:
    backend.basePath / queue

method enqueue*(backend: MemoryBackend; queue: string; payload: JsonNode) {.gcsafe.} =
  {.cast(gcsafe).}:
    backend.jobs.add((queue, payload))

method claimDue*(
  backend: MemoryBackend; queue: string; blockedTasks: openArray[string]
): ClaimedJob {.gcsafe.} =
  {.cast(gcsafe).}:
    var bestIndex = -1
    var bestPriority = MaxPriority + 1

    for i, job in backend.jobs:
      if job.queue == queue and isJobDue($job.payload) and
          not job.payload.isBlockedTask(blockedTasks):
        let priority = jobPriority(job.payload)
        if bestIndex < 0 or priority < bestPriority:
          bestIndex = i
          bestPriority = priority

    if bestIndex >= 0:
      let payload = backend.jobs[bestIndex].payload
      backend.jobs.delete(bestIndex)
      result = ClaimedJob(id: payload["id"].getStr(), payload: payload)

method cancel*(backend: MemoryBackend; queue: string; jobId: string): bool {.gcsafe.} =
  {.cast(gcsafe).}:
    for i, job in backend.jobs:
      if job.queue == queue and job.payload["id"].getStr() == jobId:
        backend.jobs.delete(i)
        return true

method discardMissed*(backend: MemoryBackend; queue: string): int {.gcsafe.} =
  {.cast(gcsafe).}:
    var kept: seq[MemoryJob] = @[]
    for job in backend.jobs:
      if job.queue != queue or not isJobDue($job.payload):
        kept.add(job)
      else:
        inc result
        let schedule =
          if "schedule" in job.payload: scheduleFromJson(job.payload["schedule"])
          else: JobSchedule(kind: skOnce)
        if schedule.kind != skOnce:
          var payload = job.payload
          payload["runAt"] = %computeNextRunAt(schedule)
          kept.add((job.queue, payload))
    backend.jobs = kept

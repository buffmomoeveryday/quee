import std/[json, math, os, random, times]
import ./[backend, priority, schedule, types]

type
  MemoryJob = object
    queue: string
    payload: JsonNode
    state: string
    leasedUntil: int64
    leaseId: string
    attempts: int
    lastError: string

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
    backend.jobs.add(MemoryJob(queue: queue, payload: payload, state: "queued"))

proc nowMillis(): int64 =
  int64(epochTime() * 1000.0)

proc newLeaseId(): string =
  $rand(high(int)) & "-" & $rand(high(int)) & "-" & $rand(high(int))

proc nextRetryRunAt(attempts, retryDelayMs: int; retryBackoff: float): float =
  if retryDelayMs <= 0:
    epochTime()
  else:
    let multiplier = pow(retryBackoff, max(attempts - 1, 0).float)
    epochTime() + (retryDelayMs.float * multiplier / 1000.0)

method claimDue*(
  backend: MemoryBackend; queue: string; blockedTasks: openArray[string]; leaseTimeoutMs: int
): ClaimedJob {.gcsafe.} =
  {.cast(gcsafe).}:
    let nowMs = nowMillis()
    var bestIndex = -1
    var bestPriority = MaxPriority + 1

    for i, job in backend.jobs:
      if job.queue == queue and
          (job.state == "queued" or (job.state == "running" and job.leasedUntil <= nowMs)) and
          isJobDue($job.payload) and
          not job.payload.isBlockedTask(blockedTasks):
        let priority = jobPriority(job.payload)
        if bestIndex < 0 or priority < bestPriority:
          bestIndex = i
          bestPriority = priority

    if bestIndex >= 0:
      backend.jobs[bestIndex].state = "running"
      backend.jobs[bestIndex].leasedUntil = nowMs + leaseTimeoutMs.int64
      backend.jobs[bestIndex].leaseId = newLeaseId()
      inc backend.jobs[bestIndex].attempts
      let payload = backend.jobs[bestIndex].payload
      result = ClaimedJob(
        id: payload["id"].getStr(),
        leaseId: backend.jobs[bestIndex].leaseId,
        payload: payload,
      )

method complete*(
  backend: MemoryBackend; queue: string; jobId: string; leaseId: string; nextPayload: JsonNode = nil
) {.gcsafe.} =
  {.cast(gcsafe).}:
    var completed = false
    for i, job in backend.jobs:
      if job.queue == queue and job.state == "running" and job.leaseId == leaseId and
          job.payload["id"].getStr() == jobId:
        backend.jobs.delete(i)
        completed = true
        break
    if completed and nextPayload != nil:
      backend.jobs.add(MemoryJob(queue: queue, payload: nextPayload, state: "queued"))

method release*(backend: MemoryBackend; queue: string; jobId: string; leaseId: string) {.gcsafe.} =
  {.cast(gcsafe).}:
    for job in backend.jobs.mitems:
      if job.queue == queue and job.state == "running" and job.leaseId == leaseId and
          job.payload["id"].getStr() == jobId:
        job.state = "queued"
        job.leasedUntil = 0
        job.leaseId = ""
        break

method fail*(
  backend: MemoryBackend;
  queue: string;
  jobId: string;
  leaseId: string;
  maxAttempts: int;
  retryDelayMs: int;
  retryBackoff: float;
  errorMessage: string;
) {.gcsafe.} =
  {.cast(gcsafe).}:
    for job in backend.jobs.mitems:
      if job.queue == queue and job.state == "running" and job.leaseId == leaseId and
          job.payload["id"].getStr() == jobId:
        job.lastError = errorMessage
        if job.attempts >= maxAttempts:
          job.state = "failed"
          job.leasedUntil = 0
          job.leaseId = ""
        else:
          job.payload["runAt"] = %nextRetryRunAt(job.attempts, retryDelayMs, retryBackoff)
          job.state = "queued"
          job.leasedUntil = 0
          job.leaseId = ""
        break

method renewLease*(
  backend: MemoryBackend; queue: string; jobId: string; leaseId: string; leaseTimeoutMs: int
): bool {.gcsafe.} =
  {.cast(gcsafe).}:
    let leasedUntil = nowMillis() + leaseTimeoutMs.int64
    for job in backend.jobs.mitems:
      if job.queue == queue and job.state == "running" and job.leaseId == leaseId and
          job.payload["id"].getStr() == jobId:
        job.leasedUntil = leasedUntil
        return true

method cancel*(backend: MemoryBackend; queue: string; jobId: string): bool {.gcsafe.} =
  {.cast(gcsafe).}:
    for i, job in backend.jobs:
      if job.queue == queue and job.state == "queued" and job.payload["id"].getStr() == jobId:
        backend.jobs.delete(i)
        return true

method listFailed*(backend: MemoryBackend; queue: string): seq[FailedJob] {.gcsafe.} =
  {.cast(gcsafe).}:
    for job in backend.jobs:
      if job.queue == queue and job.state == "failed":
        result.add(FailedJob(
          id: job.payload["id"].getStr(),
          queue: job.queue,
          taskName: job.payload["taskName"].getStr(),
          attempts: job.attempts,
          lastError: job.lastError,
          payload: job.payload,
        ))

method retryFailed*(backend: MemoryBackend; queue: string; jobId: string): bool {.gcsafe.} =
  {.cast(gcsafe).}:
    for job in backend.jobs.mitems:
      if job.queue == queue and job.state == "failed" and job.payload["id"].getStr() == jobId:
        job.state = "queued"
        job.leasedUntil = 0
        job.leaseId = ""
        job.attempts = 0
        job.lastError = ""
        return true

method deleteFailed*(backend: MemoryBackend; queue: string; jobId: string): bool {.gcsafe.} =
  {.cast(gcsafe).}:
    for i, job in backend.jobs:
      if job.queue == queue and job.state == "failed" and job.payload["id"].getStr() == jobId:
        backend.jobs.delete(i)
        return true

method discardMissed*(backend: MemoryBackend; queue: string): int {.gcsafe.} =
  {.cast(gcsafe).}:
    var kept: seq[MemoryJob] = @[]
    for job in backend.jobs:
      if job.queue != queue or job.state != "queued" or not isJobDue($job.payload):
        kept.add(job)
      else:
        inc result
        let schedule =
          if "schedule" in job.payload: scheduleFromJson(job.payload["schedule"])
          else: JobSchedule(kind: skOnce)
        if schedule.kind != skOnce:
          var payload = job.payload
          payload["runAt"] = %computeNextRunAt(schedule)
          kept.add(MemoryJob(queue: job.queue, payload: payload, state: "queued"))
    backend.jobs = kept

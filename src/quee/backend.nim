import std/[json, strformat]

type
  BackendKind* = enum
    bkSqlite
    bkMemory

  FailedJob* = object
    ## A job that exhausted its retry budget and is waiting for operator action.
    id*: string
    queue*: string
    taskName*: string
    attempts*: int
    lastError*: string
    payload*: JsonNode

  ClaimedJob* = object
    ## A job atomically leased by a backend.
    ##
    ## Return the empty/default value when no due job is available.
    id*: string
    leaseId*: string
    payload*: JsonNode

  QueueBackend* = ref object of RootObj
    ## Storage backend interface.
    ##
    ## Custom backends should atomically lease a due job in ``claimDue`` so
    ## multiple workers cannot run the same job. Leased jobs must become
    ## claimable again after ``leaseTimeoutMs`` if they are not completed.

method setup*(backend: QueueBackend; basePath: string; queues: openArray[string]) {.base, gcsafe.} =
  ## Open/create backend storage for the configured queues.
  raise newException(CatchableError, "backend setup is not implemented")

method close*(backend: QueueBackend) {.base, gcsafe.} =
  ## Release backend resources. Override for sockets, database handles, etc.
  discard

method storagePath*(backend: QueueBackend; queue: string): string {.base, gcsafe.} =
  ## Human-readable storage path/identifier for a queue.
  raise newException(CatchableError, "backend storagePath is not implemented")

method enqueue*(backend: QueueBackend; queue: string; payload: JsonNode) {.base, gcsafe.} =
  ## Persist a new queued job payload.
  raise newException(CatchableError, "backend enqueue is not implemented")

method claimDue*(
  backend: QueueBackend; queue: string; blockedTasks: openArray[string]; leaseTimeoutMs: int
): ClaimedJob {.base, gcsafe.} =
  ## Atomically lease one due job from a queue.
  raise newException(CatchableError, "backend claimDue is not implemented")

method requeue*(backend: QueueBackend; queue: string; payload: JsonNode) {.base, gcsafe.} =
  ## Persist a recurring job's next run. Most backends can reuse ``enqueue``.
  backend.enqueue(queue, payload)

method complete*(
  backend: QueueBackend; queue: string; jobId: string; leaseId: string; nextPayload: JsonNode = nil
) {.base, gcsafe.} =
  ## Complete a leased job. Delete one-shot jobs, or atomically replace a
  ## recurring job with ``nextPayload``.
  raise newException(CatchableError, "backend complete is not implemented")

method release*(backend: QueueBackend; queue: string; jobId: string; leaseId: string) {.base, gcsafe.} =
  ## Release a leased job after handler failure so it can be retried.
  raise newException(CatchableError, "backend release is not implemented")

method fail*(
  backend: QueueBackend;
  queue: string;
  jobId: string;
  leaseId: string;
  maxAttempts: int;
  retryDelayMs: int;
  retryBackoff: float;
  errorMessage: string;
) {.base, gcsafe.} =
  ## Record a leased job failure. Queue it for retry using the retry policy
  ## while attempts remain, otherwise move it to a terminal failed state.
  raise newException(CatchableError, "backend fail is not implemented")

method renewLease*(
  backend: QueueBackend; queue: string; jobId: string; leaseId: string; leaseTimeoutMs: int
): bool {.base, gcsafe.} =
  ## Extend a leased job owned by the current lease token.
  false

method cancel*(backend: QueueBackend; queue: string; jobId: string): bool {.base, gcsafe.} =
  ## Remove a queued job by id. Return false if not found or unsupported.
  false

method listFailed*(backend: QueueBackend; queue: string): seq[FailedJob] {.base, gcsafe.} =
  ## Return terminal failed jobs for ``queue``. Unsupported backends return an empty list.
  @[]

method retryFailed*(backend: QueueBackend; queue: string; jobId: string): bool {.base, gcsafe.} =
  ## Move a terminal failed job back to the queued state. Return false if not found or unsupported.
  false

method deleteFailed*(backend: QueueBackend; queue: string; jobId: string): bool {.base, gcsafe.} =
  ## Delete a terminal failed job. Return false if not found or unsupported.
  false

method discardMissed*(backend: QueueBackend; queue: string): int {.base, gcsafe.} =
  ## Drop/advance jobs that were due before startup without running them.
  raise newException(CatchableError, "backend discardMissed is not implemented")

proc backendNotConfigured*(): ref CatchableError =
  newException(CatchableError, "Quee backend is not configured; call initQuee first")

proc backendName*(kind: BackendKind): string =
  case kind
  of bkSqlite: "sqlite"
  of bkMemory: "memory"

proc unknownBackend*(kind: BackendKind): ref CatchableError =
  newException(CatchableError, &"unknown backend: {backendName(kind)}")

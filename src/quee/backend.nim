import std/[json, strformat]

type
  BackendKind* = enum
    bkLimdb
    bkSqlite

  ClaimedJob* = object
    ## A job atomically claimed by a backend.
    ##
    ## Return the empty/default value when no due job is available.
    id*: string
    payload*: JsonNode

  QueueBackend* = ref object of RootObj
    ## Storage backend interface.
    ##
    ## Custom backends should atomically remove a due job from storage in
    ## ``claimDue`` so multiple workers cannot run the same job.

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

method claimDue*(backend: QueueBackend; queue: string): ClaimedJob {.base, gcsafe.} =
  ## Atomically claim and remove one due job from a queue.
  raise newException(CatchableError, "backend claimDue is not implemented")

method requeue*(backend: QueueBackend; queue: string; payload: JsonNode) {.base, gcsafe.} =
  ## Persist a recurring job's next run. Most backends can reuse ``enqueue``.
  backend.enqueue(queue, payload)

method cancel*(backend: QueueBackend; queue: string; jobId: string): bool {.base, gcsafe.} =
  ## Remove a queued job by id. Return false if not found or unsupported.
  false

method discardMissed*(backend: QueueBackend; queue: string): int {.base, gcsafe.} =
  ## Drop/advance jobs that were due before startup without running them.
  raise newException(CatchableError, "backend discardMissed is not implemented")

proc backendNotConfigured*(): ref CatchableError =
  newException(CatchableError, "Quee backend is not configured; call initQuee first")

proc backendName*(kind: BackendKind): string =
  case kind
  of bkLimdb: "limdb"
  of bkSqlite: "sqlite"

proc unknownBackend*(kind: BackendKind): ref CatchableError =
  newException(CatchableError, &"unknown backend: {backendName(kind)}")

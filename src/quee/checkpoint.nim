import std/json
import ./registry

type
  CheckpointError* = object of CatchableError
    ## Raised when a checkpoint cannot be replayed or saved.

  StoredCheckpoint = object
    name: string
    value: JsonNode

var replayCursor {.threadvar.}: int
var checkpointDepth {.threadvar.}: int
var checkpointsActive {.threadvar.}: bool
var pendingReplay {.threadvar.}: JsonNode
var recorded {.threadvar.}: seq[StoredCheckpoint]

proc beginCheckpoints*(payload: JsonNode) {.gcsafe.} =
  ## Load completed checkpoints for the job about to run.
  {.cast(gcsafe).}:
    replayCursor = 0
    checkpointDepth = 0
    pendingReplay = nil
    recorded = @[]
    checkpointsActive = true
    if payload != nil and "checkpoints" in payload and payload["checkpoints"].kind == JArray:
      for node in payload["checkpoints"]:
        if node.kind != JObject or "name" notin node:
          continue
        let stored =
          if "result" in node and node["result"] != nil: node["result"].copy()
          else: newJNull()
        recorded.add(StoredCheckpoint(name: node["name"].getStr(), value: stored))

proc clearCheckpoints*() {.gcsafe.} =
  ## Drop checkpoint replay state for the current worker thread.
  {.cast(gcsafe).}:
    replayCursor = 0
    checkpointDepth = 0
    pendingReplay = nil
    recorded = @[]
    checkpointsActive = false

proc lastCheckpointName*(payload: JsonNode): string =
  ## Name of the last completed checkpoint stored on a job payload.
  if payload == nil or "checkpoints" notin payload:
    return ""
  let items = payload["checkpoints"]
  if items.kind != JArray or items.len == 0:
    return ""
  let last = items[^1]
  if last.kind == JObject and "name" in last:
    last["name"].getStr()
  else:
    ""

proc prepareCheckpoint(name: string): bool =
  ## Return true when ``name`` is the next completed step and its body must be skipped.
  if not checkpointsActive or currentJob().len == 0:
    raise newException(CheckpointError, "checkpoint called outside a running job")
  if checkpointDepth > 0:
    raise newException(CheckpointError, "checkpoints cannot be nested")
  if name.len == 0:
    raise newException(CheckpointError, "checkpoint name must not be empty")
  if replayCursor < recorded.len:
    let expected = recorded[replayCursor].name
    if expected != name:
      raise newException(
        CheckpointError,
        "expected checkpoint '" & expected & "', got '" & name & "'",
      )
    pendingReplay = recorded[replayCursor].value
    inc replayCursor
    return true
  for item in recorded:
    if item.name == name:
      raise newException(CheckpointError, "duplicate checkpoint '" & name & "'")
  pendingReplay = nil
  false

proc pushCheckpointFrame() =
  inc checkpointDepth

proc popCheckpointFrame() =
  if checkpointDepth > 0:
    dec checkpointDepth

proc checkpointPending(): JsonNode =
  if pendingReplay == nil: newJNull() else: pendingReplay

proc commitCheckpoint(name: string; value: JsonNode) =
  let stored = if value == nil: newJNull() else: value
  if not saveJobCheckpoint(name, stored):
    raise newException(
      CheckpointError,
      "lost lease while saving checkpoint '" & name & "'",
    )
  recorded.add(StoredCheckpoint(name: name, value: stored.copy()))
  replayCursor = recorded.len

proc runCheckpoint(name: string; body: proc()) =
  let replay = prepareCheckpoint(name)
  pushCheckpointFrame()
  try:
    if not replay:
      body()
      commitCheckpoint(name, newJNull())
  finally:
    popCheckpointFrame()

proc runCheckpoint[T](name: string; body: proc(): T): T =
  let replay = prepareCheckpoint(name)
  pushCheckpointFrame()
  try:
    if replay:
      result = checkpointPending().to(T)
    else:
      result = body()
      commitCheckpoint(name, %*result)
  finally:
    popCheckpointFrame()

template checkpoint*(name: string; body: untyped): auto =
  ## Run ``body`` once for the current job and store its JSON result.
  ##
  ## A later retry or restart skips ``body`` and returns the stored result.
  ## The step that was still running is executed again. Calls must follow the
  ## saved order, must not be nested, and must run inside a job on the worker
  ## thread. The result must be JSON-serializable. A body that returns nothing
  ## stores ``null``.
  bind runCheckpoint
  runCheckpoint(name, proc(): auto {.closure.} = body)

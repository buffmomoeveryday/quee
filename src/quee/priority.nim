import std/[json, strformat]

const
  MinPriority* = 1
  MaxPriority* = 10
  DefaultPriority* = 5

proc validatePriority*(p: int) =
  if p < MinPriority or p > MaxPriority:
    raise newException(
      ValueError,
      &"priority must be between {MinPriority} (highest) and {MaxPriority} (lowest), got {p}",
    )

const UnsetPriority* = -1

proc resolvePriority*(runtime: int; taskDefault: int): int =
  ## ``priority=`` at enqueue → task ``priority N`` line → default (5). ``UnsetPriority`` (-1) means unset.
  if runtime == UnsetPriority:
    if taskDefault != 0:
      validatePriority(taskDefault)
      taskDefault
    else:
      DefaultPriority
  else:
    validatePriority(runtime)
    runtime

proc jobPriority*(payload: JsonNode): int =
  if "priority" in payload:
    let p = payload["priority"].getInt(DefaultPriority)
    validatePriority(p)
    p
  else:
    DefaultPriority

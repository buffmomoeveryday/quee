## Stubs so `queue "name"` and `priority N` parse inside `task` bodies. The `task` macro strips these.
proc queue*(name: static string) = discard
proc priority*(level: static int) = discard

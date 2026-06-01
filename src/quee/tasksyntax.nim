## Stubs so task metadata clauses parse inside `task` bodies. The `task` macro strips these.
proc queue*(name: static string) = discard
proc priority*(level: static int) = discard
proc concurrency*(limit: static int) = discard

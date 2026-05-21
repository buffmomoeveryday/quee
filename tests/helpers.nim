import std/[os, random, strformat, times]
import quee

proc uniqueTestDb*(): string =
  let base = getTempDir() / "quee_test"
  createDir base
  result = base / &"{epochTime()}_{rand(1_000_000)}"

proc setupQuee*(queues: openArray[string] = ["default"]): string =
  result = uniqueTestDb()
  initQuee(result, queues = queues)

proc teardownQuee*(dbPath: string) =
  if dirExists(dbPath):
    removeDir(dbPath)

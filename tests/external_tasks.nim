import quee

var externalHits* = 0

task externalTask*(name: string):
  if name == "imported":
    inc externalHits

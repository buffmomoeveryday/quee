import std/[json, macros, strformat]
import ./[types, registry, builder, priority]

proc calleeName(node: NimNode): string =
  if node.kind == nnkIdent:
    $node
  elif node.kind == nnkSym:
    $node
  else:
    ""

proc extractTaskMetadata(body: NimNode): (NimNode, NimNode, NimNode, NimNode) =
  ## Strip leading metadata lines; return literals and remaining body.
  result = (
    newLit("default"),
    newLit(DefaultPriority),
    newLit(UnlimitedTaskConcurrency),
    body,
  )

  if body.kind != nnkStmtList:
    return

  var i = 0
  var queueLit = newLit("default")
  var priorityLit = newLit(DefaultPriority)
  var concurrencyLit = newLit(UnlimitedTaskConcurrency)
  while i < body.len:
    let stmt = body[i]
    if stmt.kind notin {nnkCall, nnkCommand} or stmt.len < 2:
      break
    let name = calleeName(stmt[0])
    if name == "queue":
      if stmt[1].kind != nnkStrLit:
        error "queue clause requires a string literal, e.g. queue \"emails\"", stmt[1]
      queueLit = stmt[1]
      inc i
    elif name == "priority":
      if stmt[1].kind != nnkIntLit:
        error "priority clause requires an integer literal 1–10, e.g. priority 3", stmt[1]
      let p = stmt[1].intVal
      if p < MinPriority or p > MaxPriority:
        error &"priority must be {MinPriority}..{MaxPriority}, got {p}", stmt[1]
      priorityLit = stmt[1]
      inc i
    elif name == "concurrency":
      if stmt[1].kind != nnkIntLit:
        error "concurrency clause requires an integer literal, e.g. concurrency 2", stmt[1]
      let limit = stmt[1].intVal
      if limit < 1 or limit > MaxWorkerConcurrency:
        error &"concurrency must be 1..{MaxWorkerConcurrency}, got {limit}", stmt[1]
      concurrencyLit = stmt[1]
      inc i
    else:
      break

  var rest = newStmtList()
  while i < body.len:
    rest.add(body[i])
    inc i

  result = (queueLit, priorityLit, concurrencyLit, rest)

macro task*(head: untyped, body: untyped): untyped =
  ## Define a background task and a `let` value for enqueueing.
  ##
  ## Optional leading lines in the body::
  ##
  ##   queue "emails"
  ##   priority 3
  ##   concurrency 2
  ##
  ## Override at enqueue: ``sendEmail.enqueue(addr, queue = "urgent", priority = 1)``
  let taskNameNode = head[0]
  let taskNameStr = newLit($taskNameNode)
  let typeName = ident($taskNameNode & "Task")

  let (defaultQueueLit, defaultPriorityLit, taskConcurrencyLit, handlerBody) =
    extractTaskMetadata(body)

  let jsonNodeSym = bindSym"JsonNode"
  let jsonIndexSym = bindSym"[]"
  let getStrSym = bindSym"getStr"
  let getIntSym = bindSym"getInt"
  let getFloatSym = bindSym"getFloat"
  let getBoolSym = bindSym"getBool"
  let jsonPackSym = bindSym"%*"
  let jobBuilderSym = bindSym"JobBuilder"
  let newJobBuilderSym = bindSym"newJobBuilder"
  let resolveQueueSym = bindSym"resolveQueue"
  let resolvePrioritySym = bindSym"resolvePriority"

  var paramDefs: seq[(NimNode, NimNode)]

  for i in 1 ..< head.len:
    let p = head[i]
    case p.kind
    of nnkExprColonExpr:
      paramDefs.add((p[0], p[1]))
    of nnkIdentDefs:
      let ptype = p[^2]
      for j in 0 ..< p.len - 2:
        paramDefs.add((p[j], ptype))
    else:
      error "Unexpected param kind: " & $p.kind, p

  let argsIdent = ident("args")
  let handlerSym = genSym(nskProc, $taskNameNode & "Handler")
  var handlerBodyStmts = newStmtList()

  for (pname, ptype) in paramDefs:
    let key = newLit($pname)
    let getterSym =
      case $ptype
      of "string": getStrSym
      of "int": getIntSym
      of "float": getFloatSym
      of "bool": getBoolSym
      else: getStrSym
    handlerBodyStmts.add(newLetStmt(pname, newCall(getterSym, newCall(jsonIndexSym, argsIdent, key))))

  for stmt in handlerBody:
    handlerBodyStmts.add(stmt)

  let handlerProc = newProc(
    name = handlerSym,
    params = [newEmptyNode(), newIdentDefs(argsIdent, jsonNodeSym)],
    body = handlerBodyStmts,
  )

  var jsonTableConstr = newNimNode(nnkTableConstr)
  for (pname, _) in paramDefs:
    jsonTableConstr.add(newNimNode(nnkExprColonExpr).add(newLit($pname), pname))

  let jsonPackExpr = newCall(jsonPackSym, jsonTableConstr)

  let queueParam = ident("queue")
  let priorityParam = ident("priority")
  let enqueueSym = ident("enqueue")
  var enqueueParams: seq[NimNode] = @[
    jobBuilderSym,
    newIdentDefs(ident("self"), typeName),
  ]
  for (pname, ptype) in paramDefs:
    enqueueParams.add(newIdentDefs(pname, ptype))
  enqueueParams.add(newIdentDefs(queueParam, ident("string"), newLit("")))
  enqueueParams.add(newIdentDefs(priorityParam, ident("int"), newLit(UnsetPriority)))

  let enqueueBody = newCall(
    newJobBuilderSym,
    taskNameStr,
    jsonPackExpr,
    newCall(resolveQueueSym, queueParam, newDotExpr(ident("self"), ident("defaultQueue"))),
    newCall(
      resolvePrioritySym,
      priorityParam,
      newDotExpr(ident("self"), ident("defaultPriority")),
    ),
  )

  let enqueueProc = newProc(
    name = enqueueSym,
    params = enqueueParams,
    body = enqueueBody,
  )

  result = quote do:
    type `typeName`* = object
      defaultQueue*: string
      defaultPriority*: int
      taskConcurrency*: int
    `handlerProc`
    `enqueueProc`
    registerTask(
      `taskNameStr`,
      `defaultQueueLit`,
      `defaultPriorityLit`,
      `taskConcurrencyLit`,
      `handlerSym`,
    )
    let `taskNameNode`* = `typeName`(
      defaultQueue: `defaultQueueLit`,
      defaultPriority: `defaultPriorityLit`,
      taskConcurrency: `taskConcurrencyLit`,
    )

import std/[base64, os, sequtils, strformat, strutils, times]
import karax/[karaxdsl, vdom]
import mummy, mummy/routers
import ./[dashboardstyles, registry, types]

type
  DashboardServerConfig = object
    host: string
    port: int
    path: string

var dashboardThread: Thread[DashboardServerConfig]
var dashboardStarted = false

proc htmlResponse(request: Request; body: string; status = 200; contentType = "text/html") =
  var headers: HttpHeaders
  headers["Content-Type"] = contentType
  request.respond(status, headers, body)

proc unauthorized(request: Request) =
  var headers: HttpHeaders
  headers["Content-Type"] = "text/plain"
  headers["WWW-Authenticate"] = "Basic realm=\"Quee Dashboard\""
  request.respond(401, headers, "Authentication required")

proc missingCredentials(request: Request) =
  request.htmlResponse(
    "Set QUEE_DASHBOARD_USERNAME and QUEE_DASHBOARD_PASSWORD to enable the dashboard.",
    503,
    "text/plain",
  )

proc basicAuthAccepted*(authorization, username, password: string): bool =
  if username.len == 0 or password.len == 0:
    return false
  if not authorization.startsWith("Basic "):
    return false
  try:
    let decoded = decode(authorization[6..^1])
    decoded == username & ":" & password
  except CatchableError:
    false

proc authorize(request: Request): bool =
  let username = getEnv("QUEE_DASHBOARD_USERNAME")
  let password = getEnv("QUEE_DASHBOARD_PASSWORD")
  if username.len == 0 or password.len == 0:
    request.missingCredentials()
    return false
  if "Authorization" notin request.headers or
      not basicAuthAccepted(request.headers["Authorization"], username, password):
    request.unauthorized()
    return false
  true

proc formatTime(ts: float): string =
  if ts <= 0:
    return "now"
  fromUnixFloat(ts).local.format("yyyy-MM-dd HH:mm:ss")

proc stateBadge(state: string): VNode =
  let classes =
    case state
    of "running": "badge bg-indigo-50 text-indigo-700"
    of "failed": "badge bg-rose-50 text-rose-700"
    else: "badge bg-emerald-50 text-emerald-700"
  buildHtml(span(class = classes)):
    text state

proc queueControls(queue: QueueStats; basePath: string): VNode =
  let action =
    if queue.paused: "resume" else: "pause"
  let label =
    if queue.paused: "Resume" else: "Pause"
  let buttonNode = tree(VNodeKind.button)
  buttonNode.class = "btn btn-muted"
  buttonNode.add(text(label))
  buttonNode.setAttr("type", "submit")
  buttonNode.setAttr("hx-post", &"{basePath}/queues/{queue.name}/{action}")
  buttonNode.setAttr("hx-target", "#queues-panel")
  buttonNode.setAttr("hx-swap", "outerHTML")
  let formNode = tree(VNodeKind.form)
  formNode.setAttr("action", &"{basePath}/queues/{queue.name}/{action}")
  formNode.add(buttonNode)
  formNode.setAttr("method", "post")
  formNode

proc renderSummary*(basePath: string): string =
  let stats = queueStats()
  let jobs = listJobs()
  let queued = stats.mapIt(it.queued).foldl(a + b, 0)
  let running = stats.mapIt(it.running).foldl(a + b, 0)
  let failed = stats.mapIt(it.failed).foldl(a + b, 0)
  let scheduled = stats.mapIt(it.scheduled).foldl(a + b, 0)
  let node = buildHtml(tdiv(id = "summary-panel", class = "grid grid-cols-1 md:grid-cols-4 gap-4")):
    for item in [
      ("Queued", queued, "bg-emerald-50 text-emerald-700"),
      ("Running", running, "bg-indigo-50 text-indigo-700"),
      ("Failed", failed, "bg-rose-50 text-rose-700"),
      ("Scheduled", scheduled, "bg-amber-50 text-amber-700"),
    ]:
      tdiv(class = "rounded-lg border border-slate-200 bg-white p-6 shadow-sm"):
        p(class = "text-sm font-medium text-slate-500"):
          text item[0]
        p(class = "mt-1 text-3xl font-bold tabular-nums text-slate-900"):
          text $item[1]
        span(class = "badge " & item[2]):
          text "live"
    tdiv(class = "hidden"):
      text $jobs.len
  node.setAttr("hx-get", basePath & "/partials/summary")
  node.setAttr("hx-trigger", "every 3s")
  node.setAttr("hx-swap", "outerHTML")
  $node

proc renderQueues*(basePath: string): string =
  let stats = queueStats()
  let node = buildHtml(tdiv(id = "queues-panel", class = "rounded-lg border border-slate-200 bg-white shadow-sm overflow-hidden")):
    tdiv(class = "px-5 py-4 flex items-center justify-between"):
      h2(class = "text-lg font-semibold"):
        text "Queues"
    tdiv(class = "overflow-x-auto"):
      table(class = "min-w-full divide-y divide-slate-100"):
        thead(class = "bg-slate-50"):
          tr:
            for name in ["Queue", "Queued", "Running", "Scheduled", "Failed", "Status", "Action"]:
              th(class = "px-5 py-3 text-xs font-semibold uppercase tracking-wide text-slate-500"):
                text name
        tbody(class = "divide-y divide-slate-100"):
          for queue in stats:
            tr:
              td(class = "px-5 py-3 font-medium"):
                text queue.name
              td(class = "px-5 py-3 tabular-nums"):
                text $queue.queued
              td(class = "px-5 py-3 tabular-nums"):
                text $queue.running
              td(class = "px-5 py-3 tabular-nums"):
                text $queue.scheduled
              td(class = "px-5 py-3 tabular-nums"):
                text $queue.failed
              td(class = "px-5 py-3"):
                span(class = if queue.paused: "badge bg-amber-50 text-amber-700" else: "badge bg-emerald-50 text-emerald-700"):
                  text(if queue.paused: "paused" else: "active")
              td(class = "px-5 py-3"):
                verbatim $queueControls(queue, basePath)
  node.setAttr("hx-get", basePath & "/partials/queues")
  node.setAttr("hx-trigger", "every 3s")
  node.setAttr("hx-swap", "outerHTML")
  $node

proc renderTasks*(basePath: string): string =
  let tasks = listTasks()
  let node = buildHtml(tdiv(id = "tasks-panel", class = "rounded-lg border border-slate-200 bg-white shadow-sm overflow-hidden")):
    tdiv(class = "px-5 py-4"):
      h2(class = "text-lg font-semibold"):
        text "Tasks"
    tdiv(class = "overflow-x-auto"):
      table(class = "min-w-full divide-y divide-slate-100"):
        thead(class = "bg-slate-50"):
          tr:
            for name in ["Task", "Queue", "Priority", "Concurrency", "Running"]:
              th(class = "px-5 py-3 text-xs font-semibold uppercase tracking-wide text-slate-500"):
                text name
        tbody(class = "divide-y divide-slate-100"):
          for task in tasks:
            tr:
              td(class = "px-5 py-3 font-medium"):
                text task.name
              td(class = "px-5 py-3"):
                text task.defaultQueue
              td(class = "px-5 py-3 tabular-nums"):
                text $task.defaultPriority
              td(class = "px-5 py-3"):
                text(if task.taskConcurrency == UnlimitedTaskConcurrency: "unlimited" else: $task.taskConcurrency)
              td(class = "px-5 py-3 tabular-nums"):
                text $runningTaskCount(task.name)
  node.setAttr("hx-get", basePath & "/partials/tasks")
  node.setAttr("hx-trigger", "every 3s")
  node.setAttr("hx-swap", "outerHTML")
  $node

proc actionForm(basePath, action, queue, jobId, label, classes: string): VNode =
  let buttonNode = tree(VNodeKind.button)
  buttonNode.class = classes
  buttonNode.add(text(label))
  buttonNode.setAttr("type", "submit")
  buttonNode.setAttr("hx-post", &"{basePath}/jobs/{queue}/{jobId}/{action}")
  buttonNode.setAttr("hx-target", "#jobs-panel")
  buttonNode.setAttr("hx-swap", "outerHTML")
  let formNode = tree(VNodeKind.form)
  formNode.setAttr("action", &"{basePath}/jobs/{queue}/{jobId}/{action}")
  formNode.add(buttonNode)
  formNode.setAttr("method", "post")
  formNode

proc renderJobs*(basePath, queueFilter, stateFilter: string): string =
  let jobs = listJobs().filterIt(
    (queueFilter.len == 0 or it.queue == queueFilter) and
      (stateFilter.len == 0 or it.state == stateFilter)
  )
  let node = buildHtml(tdiv(id = "jobs-panel", class = "rounded-lg border border-slate-200 bg-white shadow-sm overflow-hidden")):
    tdiv(class = "px-5 py-4 flex items-center justify-between"):
      h2(class = "text-lg font-semibold"):
        text "Jobs"
    tdiv(class = "overflow-x-auto"):
      table(class = "min-w-full divide-y divide-slate-100"):
        thead(class = "bg-slate-50"):
          tr:
            for name in ["Job", "Task", "Queue", "State", "Priority", "Attempts", "Run At", "Error", "Actions"]:
              th(class = "px-5 py-3 text-xs font-semibold uppercase tracking-wide text-slate-500"):
                text name
        tbody(class = "divide-y divide-slate-100"):
          for job in jobs:
            tr:
              td(class = "px-5 py-3 max-w-xs truncate"):
                text job.id
              td(class = "px-5 py-3"):
                text job.taskName
              td(class = "px-5 py-3"):
                text job.queue
              td(class = "px-5 py-3"):
                verbatim $stateBadge(job.state)
              td(class = "px-5 py-3 tabular-nums"):
                text $job.priority
              td(class = "px-5 py-3 tabular-nums"):
                text $job.attempts
              td(class = "px-5 py-3 text-sm text-slate-600"):
                text formatTime(job.runAt)
              td(class = "px-5 py-3 max-w-xs truncate text-sm text-slate-600"):
                text job.lastError
              td(class = "px-5 py-3"):
                tdiv(class = "flex gap-2"):
                  if job.state == "failed":
                    verbatim $actionForm(basePath, "retry", job.queue, job.id, "Retry", "btn btn-primary")
                    verbatim $actionForm(basePath, "delete", job.queue, job.id, "Delete", "btn btn-danger")
                  elif job.state == "queued" or job.state == "running":
                    verbatim $actionForm(basePath, "cancel", job.queue, job.id, "Cancel", "btn btn-warning")
  node.setAttr("hx-get", basePath & "/partials/jobs")
  node.setAttr("hx-trigger", "every 3s")
  node.setAttr("hx-swap", "outerHTML")
  $node

proc renderPage*(basePath: string): string =
  let node = buildHtml(html):
    head:
      meta(charset = "utf-8")
      meta(name = "viewport", content = "width=device-width, initial-scale=1")
      title:
        text "Quee Monitoring"
      link(rel = "stylesheet", href = basePath & "/assets/dashboard.css")
      script(src = "https://unpkg.com/htmx.org@2.0.4"):
        text ""
    body:
      main(class = "min-h-screen"):
        tdiv(class = "mx-auto max-w-7xl p-6"):
          header(class = "mb-4 flex items-center justify-between"):
            tdiv:
              h1(class = "text-2xl font-bold text-slate-900"):
                text "Quee Monitoring"
              p(class = "text-sm text-slate-500"):
                text "Queues, tasks, jobs, and operator controls"
            a(class = "btn btn-muted", href = basePath):
              text "Refresh"
          verbatim renderSummary(basePath)
          tdiv(class = "mt-6 grid grid-cols-1 md:grid-cols-2 gap-6"):
            verbatim renderQueues(basePath)
            verbatim renderTasks(basePath)
          tdiv(class = "mt-6"):
            verbatim renderJobs(basePath, "", "")
  "<!DOCTYPE html>\n" & $node

proc routeBase(basePath, suffix: string): string =
  if suffix.len == 0:
    basePath
  else:
    basePath & suffix

proc handleIndex(request: Request; basePath: string) =
  if request.authorize():
    request.htmlResponse(renderPage(basePath))

proc handleCss(request: Request) =
  if request.authorize():
    request.htmlResponse(DashboardCss, 200, "text/css")

proc handleSummary(request: Request; basePath: string) =
  if request.authorize():
    request.htmlResponse(renderSummary(basePath))

proc handleQueues(request: Request; basePath: string) =
  if request.authorize():
    request.htmlResponse(renderQueues(basePath))

proc handleTasks(request: Request; basePath: string) =
  if request.authorize():
    request.htmlResponse(renderTasks(basePath))

proc handleJobs(request: Request; basePath: string) =
  if request.authorize():
    request.htmlResponse(renderJobs(
      basePath,
      request.queryParams.getOrDefault("queue", ""),
      request.queryParams.getOrDefault("state", ""),
    ))

proc handleQueueAction(request: Request; basePath: string) =
  if not request.authorize():
    return
  let queue = request.pathParams["queue"]
  let action = request.pathParams["action"]
  if action == "pause":
    pauseQueue(queue)
  elif action == "resume":
    resumeQueue(queue)
  request.htmlResponse(renderQueues(basePath))

proc handleJobAction(request: Request; basePath: string) =
  if not request.authorize():
    return
  let queue = request.pathParams["queue"]
  let jobId = request.pathParams["jobId"]
  let action = request.pathParams["action"]
  if action == "retry":
    discard retryFailedJob(jobId, queue)
  elif action == "delete":
    discard deleteFailedJob(jobId, queue)
  elif action == "cancel":
    discard cancelJob(jobId, queue)
  request.htmlResponse(renderJobs(basePath, "", ""))

proc initDashboardRouter(basePath: string): Router =
  result.get(routeBase(basePath, ""), proc(request: Request) {.gcsafe.} =
    {.cast(gcsafe).}: handleIndex(request, basePath))
  result.get(routeBase(basePath, "/assets/dashboard.css"), proc(request: Request) {.gcsafe.} =
    {.cast(gcsafe).}: handleCss(request))
  result.get(routeBase(basePath, "/partials/summary"), proc(request: Request) {.gcsafe.} =
    {.cast(gcsafe).}: handleSummary(request, basePath))
  result.get(routeBase(basePath, "/partials/queues"), proc(request: Request) {.gcsafe.} =
    {.cast(gcsafe).}: handleQueues(request, basePath))
  result.get(routeBase(basePath, "/partials/tasks"), proc(request: Request) {.gcsafe.} =
    {.cast(gcsafe).}: handleTasks(request, basePath))
  result.get(routeBase(basePath, "/partials/jobs"), proc(request: Request) {.gcsafe.} =
    {.cast(gcsafe).}: handleJobs(request, basePath))
  result.post(routeBase(basePath, "/queues/@queue/@action"), proc(request: Request) {.gcsafe.} =
    {.cast(gcsafe).}: handleQueueAction(request, basePath))
  result.post(routeBase(basePath, "/jobs/@queue/@jobId/@action"), proc(request: Request) {.gcsafe.} =
    {.cast(gcsafe).}: handleJobAction(request, basePath))

proc dashboardLoop(config: DashboardServerConfig) {.thread.} =
  let router = initDashboardRouter(config.path)
  let server = newServer(router)
  server.serve(Port(config.port), config.host)

proc startMonitoringDashboard*() =
  ## Start the standalone dashboard server once when dashboard settings are enabled.
  if dashboardStarted or not monitoringDashboardEnabled():
    return
  let config = DashboardServerConfig(
    host: monitoringDashboardHost(),
    port: monitoringDashboardPort(),
    path: monitoringDashboardPath(),
  )
  createThread(dashboardThread, dashboardLoop, config)
  dashboardStarted = true

import std/[base64, strutils, unittest]
import quee
import helpers

task dashboardTask():
  discard

suite "monitoring dashboard":
  var dbPath: string

  setup:
    dbPath = setupQuee()

  teardown:
    teardownQuee(dbPath)

  test "basic auth accepts configured credentials":
    let token = encode("admin:secret")
    check basicAuthAccepted("Basic " & token, "admin", "secret")
    check not basicAuthAccepted("Basic " & token, "admin", "wrong")
    check not basicAuthAccepted("Bearer " & token, "admin", "secret")
    check not basicAuthAccepted("Basic invalid", "admin", "secret")

  test "dashboard renders page and htmx partials":
    discard dashboardTask.enqueue().run()

    let page = renderPage("/monitor")
    check "<!DOCTYPE html>" in page
    check "Quee Monitoring" in page
    check "/monitor/assets/dashboard.css" in page
    check "htmx.org" in page

    let summary = renderSummary("/monitor")
    check "hx-get=\"/monitor/partials/summary\"" in summary
    check "Queued" in summary

    let jobs = renderJobs("/monitor", "", "")
    check "dashboardTask" in jobs
    check "hx-post=\"/monitor/jobs/default/" in jobs

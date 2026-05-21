import quee

import mummy, mummy/routers

proc toGcsafeHandler*(h: proc(request: Request)): proc(request: Request) {.gcsafe.} =
  proc (request: Request) {.gcsafe.} =
    {.cast(gcsafe).}:
      h(request)

initQuee("./mydb")

task sendEmail(email: string, subject: string):
  echo "→ Sending '", subject, "' to ", email

task generateReport(userId: string):
  echo "→ Report for user ", userId


proc objectsHandler(request: Request) =
  var headers: HttpHeaders
  headers["Content-Type"] = "text/plain"

  let email = request.pathParams["email"]
  discard sendEmail.enqueue(email, "Welcome!").run()
  request.respond(200, headers, "Object: " & email)

var router: Router
router.get("/objects/@email", toGcsafeHandler(objectsHandler))

startQuee()

let server = newServer(router)
echo "Serving on http://localhost:8080"
server.serve(Port(8080))

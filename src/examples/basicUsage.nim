import std/[os, times]
import ../quee

let dbPath = getTempDir() / "quee_example_basic"
if dirExists(dbPath):
  removeDir(dbPath)

initQuee(dbPath, pollIntervalMs = 10)

task sendEmail(email: string, subject: string):
  echo "send email: ", subject, " -> ", email

task resizeImage(imageId: int):
  echo "resize image: ", imageId

discard sendEmail.enqueue("ada@example.com", "Welcome").run()
discard resizeImage.enqueue(42).after(100.milliseconds)

echo "Draining immediate work with processOne()"
while processOne():
  discard

echo "Waiting for delayed work"
sleep(150)
while processOne():
  discard

echo "Done"

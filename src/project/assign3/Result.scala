package project.assign3

case class Result(s: Long, time0: Long, time1: Long, workP: Int) extends Serializable {
  var sum: Long = s
  var t0: Long = time0
  var t1: Long = time1
  var workerPort: Int = workP
}
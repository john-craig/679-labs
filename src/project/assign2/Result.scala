package project.assign2

case class Result(s: Double, time0: Long, time1: Long) extends Serializable {
  var sum: Double = s
  var t0: Long = time0
  var t1: Long = time1
}

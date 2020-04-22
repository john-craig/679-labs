package project.assign3
import parabond.cluster.Analysis

case class Result(a: Analysis, time0: Long, time1: Long, workP: Int) extends Serializable {
  var analysis: Analysis = a
  var t0: Long = time0
  var t1: Long = time1
  var workerPort: Int = workP
}
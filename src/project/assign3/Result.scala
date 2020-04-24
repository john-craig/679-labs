package project.assign3
import parabond.cluster.Analysis

case class Result(time_difference: Long, workP: Int) extends Serializable {
  var delta_t: Long = time_difference
  var workerPort: Int = workP
}
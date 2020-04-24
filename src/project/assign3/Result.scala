package project.assign3
import parabond.cluster.Analysis

case class Result(time_difference: Long, workP: Int, portfs: List[Int]) extends Serializable {
  val delta_t: Long = time_difference
  val workerPort: Int = workP
  val portfIds: List[Int] = portfs
}
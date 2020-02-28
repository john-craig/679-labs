package lab6
import scala.concurrent._
import ExecutionContext.Implicits.global

object FutureGLEstimator extends App {
  //val n = Int.MaxValue
  val RANGE = 1000000
  val NUM_PARTITIONS = (Int.MaxValue / RANGE)

  def IterationGL(n: Double) = {
    Math.pow(-1.0, n) / ((2.0 * n) + 1.0)
  }

  def PartialSummationGL(begin: Long, end: Long): Double = {
    (begin to end).foldLeft(0.0){(a, b) => (a + IterationGL(b))}
  }

  val futures = for(k <- 0L until NUM_PARTITIONS) yield Future {
    val lower = (k * RANGE) + 1
    val upper = (k + 1) * RANGE

    val sum = PartialSummationGL(lower, upper)

    sum * 4.0
  }

  val start = System.nanoTime()

  val pi = futures.foldLeft(4.0) { (sum, future) =>
    import scala.concurrent.duration._
    val pi = Await.result(future, 100 seconds)

    sum + pi
  }

  val end = System.nanoTime()

  //Calculate output variables
  val pi0 = 3.1415926531226095
  val t1 = 151.0
  val tn = (end - start) / 1000000000
  val n = (Runtime.getRuntime.availableProcessors() / 2)

  //I assume these were meant to be the ratio of runtimes
  //and the error between Pi values
  val r = (t1 / tn)
  val e = ((pi) / pi0) * 100.00

  println("Pi = " + pi)
  println("T1 = " + t1) // fixed from last lab
  println("TN = " + tn)
  println("N = " + n)
  println("R = " + r)
  println("e = " + e)

}

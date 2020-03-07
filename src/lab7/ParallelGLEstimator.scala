package lab7
import scala.concurrent._
import ExecutionContext.Implicits.global

object ParallelGLEstimator extends App {
  //val n = Int.MaxValue
  val RANGE = 1000000
  val NUM_PARTITIONS = (Int.MaxValue / RANGE)

  val start = System.nanoTime()

  def IterationGL(n: Double) = {
    Math.pow(-1.0, n) / ((2.0 * n) + 1.0)
  }

  def PartialSummationGL(begin: Int, end: Int): Double = {
    (begin to end).foldLeft(0.0){(a, b) => (a + IterationGL(b))}
  }

  val ranges = for(k <- 0 to NUM_PARTITIONS) yield {
    val lower: Int = k * RANGE + 1
    val upper: Int = Int.MaxValue min (k + 1) * RANGE

    (lower, upper)
  }

  val partials = ranges.par.map { partial =>
    val (lower, upper) = partial
    val sum = PartialSummationGL(lower, upper) * 4

    sum
  }

  val pi = partials.foldLeft(4.0){(a,b) => a + b}

  val end = System.nanoTime()

  //Calculate output variables
  val pi0 = 3.1415926531226095
  val t1 = 29.0
  val tn = (end - start) / 1000000000
  val n = (Runtime.getRuntime.availableProcessors() / 2)
  val r = (t1 / tn)
  val e = r / n


  println("Pi = " + pi)
  println("T1 = " + t1) // fixed from last lab
  println("TN = " + tn)
  println("N = " + n)
  println("R = " + r)
  println("e = " + e)

}

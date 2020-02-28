package lab5

object GLEstimator extends App {
  val n = Int.MaxValue
  //val pi = nums.foldLeft(0){(a,b) => (a + ((-1 ^ b) / ((2 * b) + 1)))}
  def IterationGL(n: Double) = {
    Math.pow(-1.0, n) / ((2.0 * n) + 1.0)
  }

  val start = System.nanoTime()

  def SummationGL(n: Int): Double = {
    (0 to n).foldLeft(0.0){(a,b) => (a + IterationGL(b))}
  }

  val quarter_pi = SummationGL(n)
  val pi = quarter_pi * 4.0

  val end = System.nanoTime()
  val dt = (end - start) / 1000000000

  println("Pi: " + pi)
  println("dt: " + dt)
}

package lab3

class ChildRunnable(no: Int) extends Runnable{
  override def run(): Unit = {
    val m = Thread.currentThread().getId()
    val n = no

    println("child: " + n + " " + m)
  }
}

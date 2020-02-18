package lab3

class ChildThread(no: Int) extends Thread {
  override def run(): Unit = {
    val m = this.getId()
    val n = no

    println("child: " + n + " " + m)
  }
}

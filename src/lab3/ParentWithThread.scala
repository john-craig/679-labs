package lab3

object ParentWithThread extends App {
  val numCores = Runtime.getRuntime.availableProcessors()

  val child = new Thread(new ChildThread(0))
  child.start()

  val numThreads = Thread.activeCount()

  println(numCores)
  println(numThreads)

  child.join()
}

package project.assign2

import org.apache.log4j.Logger
import parascale.actor.last.{Task, Worker}
import parascale.util._
import project.assign2.Partition
import project.assign2.Result

/**
 * Spawns workers on the localhost.
 */
object PerfectWorker extends App {
  val LOG = Logger.getLogger(getClass)

  LOG.info("started")

  // Number of hosts in this configuration
  val nhosts = getPropertyOrElse("nhosts",1)

  // One-port configuration
  val port1 = getPropertyOrElse("port", 8000)

  // If there is just one host, then the ports will include 9000 by default
  // Otherwise, if there are two hosts in this configuration, use just one
  // port which must be specified by VM options
  val ports = if(nhosts == 1) List(port1, 9000) else List(port1)

  // Spawn the worker(s).
  // Note: for initial testing with a single host, "ports" contains two ports.
  // When deploying on two hosts, "ports" will contain one port per host.
  for(port <- ports) {
    // Construction forks a thread which automatically runs the actor act method.
    new PerfectWorker(port)
  }
}

/**
 * Template worker for finding a perfect number.
 * @param port Localhost port this worker listens to
 */
class PerfectWorker(port: Int) extends Worker(port) {
  import PerfectWorker._

  /**
   * Handles actor startup after construction.
   */
  override def act: Unit = {
    val name = getClass.getSimpleName
    LOG.info("started " + name + " (id=" + id + ")")

    val RANGE = 1000000


    def IterationGL(n: Double) = {
      Math.pow(-1.0, n) / ((2.0 * n) + 1.0)
    }

    def PartialSummationGL(begin: Long, end: Long): Double = {
      (begin to end).foldLeft(0.0){(a, b) => (a + IterationGL(b))}
    }

    // Wait for inbound messages as tasks
    while (true) {
      receive match {
        // TODO: Replace the code below to implement PNF
        // It gets the partition range info from the task payload then
        // spawns futures (or uses parallel collections) to analyze the
        // partition in parallel. Finally, when done, it replies
        // with the partial sum and the time elapsed time.
        case task: Task =>
          LOG.info("got task, sending reply")
          val start = System.nanoTime()

          val partition : Partition = task.payload.asInstanceOf[Partition];
          val NUM_PARTITIONS = (partition.getRange() / RANGE).asInstanceOf[Int]


          val ranges = for(k <- 0 to NUM_PARTITIONS) yield {
             val lower: Long = k * RANGE + 1
             val upper: Long = partition.getRange() min (k + 1) * RANGE

            (lower, upper)
          }

          val partials = ranges.par.map { partial =>
            val (lower, upper) = partial
            val sum = PartialSummationGL(lower, upper) * 4

            sum
          }

          val pi = partials.foldLeft(4.0){(a,b) => a + b}
          val end = System.nanoTime()

          val result = Result(pi, start, end)
          // Send a simple reply to test the connectivity.
          sender ! result
      }
    }
  }
}
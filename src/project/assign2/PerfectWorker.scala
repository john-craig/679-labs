package project.assign2

import org.apache.log4j.Logger
import parascale.actor.last.{Task, Worker}
import parascale.util._
import project.assign2.Partition
import project.assign2.Result

import scala.concurrent._
import ExecutionContext.Implicits.global

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

    val NUM_CORES = Runtime.getRuntime.availableProcessors()
    val start = System.nanoTime()

    //Checks if a divisor is a factor of an operand
    def isFactor(divisor: Long, operand: Long): Long = {
      if ((operand % divisor == 0) && (operand != divisor)) {
        divisor
      } else{
        0L
      }
    }

    //sums the factors of the operand which are within the range
    def sumFactorsInRange(begin: Long, end: Long, operand: Long): Long = {
      val difference = end - begin

      if(difference > 1){

        val firstHalf: Long = sumFactorsInRange(begin, begin + Math.floor(difference / 2).asInstanceOf[Long], operand): Long
        val secondHalf: Long = sumFactorsInRange(begin + Math.floor(difference / 2).asInstanceOf[Long], end, operand): Long

        firstHalf + secondHalf
      } else {
        isFactor(begin, operand)
      }
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

          val payload = task.payload

          payload match {
            case payload: String => {
              sender ! "connection established!"
            }
            case payload: Partition => {
              val partition: Partition = task.payload.asInstanceOf[Partition];
              val candidate = partition.candidate
              val start = partition.start
              val ending = partition.end

              var sum = 0L

              //Having a range of variables smaller than the number of cores
              // is an edge case which the below code will not run on properly
              if (partition.getRange () < NUM_CORES) {
                val futures = for (k <- 0L until partition.getRange () + 1) yield Future {
                val sum = isFactor (k + start, candidate)

                sum
                }

                sum = futures.foldLeft (0L) {
                  (sum, future) =>
                  import scala.concurrent.duration._
                  val next_sum = Await.result (future, 100 seconds)

                  sum + next_sum
                }
              } else {
                val range = 1L max (partition.getRange () / NUM_CORES).asInstanceOf[Long]
                val length = NUM_CORES.asInstanceOf[Long] max (partition.getRange () / range) + 1

                val futures = for (k <- 0L until (length) ) yield Future {
                val lower = (k * range) + start
                val upper = (((k + 1) * range) + start)

                val sum = sumFactorsInRange (lower, upper, candidate)

                sum
              }

              sum = futures.foldLeft (0L) {
                (sum, future) =>
                import scala.concurrent.duration._
                val next_sum = Await.result (future, 1000 seconds)

                sum + next_sum
                }
              }

              val end = System.nanoTime ()

              val result = Result (sum, start, end, candidate, NUM_CORES)

              sender ! result
            }
          }
      }
    }
  }
}
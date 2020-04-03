package assign2

import org.apache.log4j.Logger
import parascale.actor.last.{Dispatcher, Task}
import parascale.util._
import project.assign2.Partition
import project.assign2.Result

import scala.collection.mutable.ListBuffer
/**
 * Spawns a dispatcher to connect to multiple workers.
 */
object PerfectDispatcher extends App {
  val LOG = Logger.getLogger(getClass)
  LOG.info("started")

  // For initial testing on a single host, use this socket.
  // When deploying on multiple hosts, use the VM argument,
  // -Dsocket=<ip address>:9000 which points to the second
  // host.
  val socket2 = getPropertyOrElse("socket","localhost:8000")

  // Construction forks a thread which automatically runs the actor act method.
  new PerfectDispatcher(List("localhost:8000", socket2))
}

/**
 * Template dispatcher which tests readiness of
 * @param sockets
 */
class PerfectDispatcher(sockets: List[String]) extends Dispatcher(sockets) {
  import PerfectDispatcher._

  /**
   * Handles actor startup after construction.
   */
  def act: Unit = {
    LOG.info("sockets to workers = "+sockets)

    (0 until sockets.length).foreach { k =>
      LOG.info("sending message to worker " + k)
      workers(k) ! "to worker(" + k + ") hello from dispatcher"
    }

    // TODO: Replace the code below to implement PNF
    // Create the partition info and put it in two separate messages,
    // one for each worker, then wait below for two replies, one from
    // each worker

    var t0 = System.nanoTime()

    val candidates = IndexedSeq(
      6L,
      28L,
      496L,
      8128L,
      33550336L,
      33550337L,
      85899869057L,
      85899869056L,
      137438691328L
    )
    var results = new ListBuffer[Result]
    var cur_candidate = 0

    //Helper function which determines all the partitions to be
    //sent to the workers based on only the current candidate
    def repartition(candidate: Long): IndexedSeq[Partition] = {
      val partitions = for(k <- 0 to sockets.length) yield {
        val range = candidate / sockets.length

        val lower: Long = k * range + 1
        val upper: Long = candidate min (k + 1) * range

        Partition(lower, upper, candidate)
      }

      (partitions)
    }

    //Helper function to actually send partitions to the workers
    def sendPartitions(partitions: IndexedSeq[Partition]): Unit =
    {
      (0 until sockets.length).foreach { k =>
        //LOG.info("sending partition info to worker " + k)
        workers(k) ! partitions(k)
      }
    }

    def checkResults(r : ListBuffer[Result]): Unit ={
      if(r.size == 2){
        handleResults(IndexedSeq(r(0), r(1)))
        results = new ListBuffer[Result]
      }
    }

    def handleResults(results: IndexedSeq[Result]): Unit = {
      val total = results.foldLeft(0L){(sum, result) =>
        val total = sum + result.sum

        total
      }

      val tn = results.foldLeft(0L){(time, result) =>
        val tn = time + (result.t1 - result.t0)

        tn
      }

      val n = results.foldLeft(0){(numCores, result) =>
        val n = numCores + result.numCores

        n
      }

      val t1 = System.nanoTime()

      LOG.info(total)

      val isPerfect = (total == results(0).candidate)

      printReport(results(0).candidate, isPerfect, tn, t1, n)
    }

    //Helper function to print the report
    def printReport(candidate: Long, result: Boolean, t1: Long, tn: Long, numCores: Int): Unit = {
      val r = (t1 / tn).asInstanceOf[Double]
      val e = (r / numCores).asInstanceOf[Double]

      LOG.info(candidate + "      " + result + "      " + t1  + "      " + tn  + "      " + r  + "      " + e)
    }

    for (k <- 0 until candidates.size){
      val partition = repartition(candidates(k))
      sendPartitions(partition)
    }

    while (true) {
      //Sends the partitions of the current candidate to the workers
      //then changes state so that it doesn't repeatedly send them over
      //and over; also increments the current candidate


      receive match {
        case task: Task if task.kind == Task.REPLY =>
          val payload = task.payload

          payload match{
            case payload: String => {
              LOG.info(payload)
            }
            case payload: Result => {
              val result = payload.asInstanceOf[Result]

              results += result

              checkResults(results)
            }
          }
      }
    }

  }
}

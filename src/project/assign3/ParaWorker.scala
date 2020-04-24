package project.assign3

import assign2.PerfectDispatcher.getClass
import org.apache.log4j.Logger
import parascale.actor.last.{Task, Worker}
import parascale.util._
import parabond.cluster._

import scala.concurrent._
import ExecutionContext.Implicits.global


object ParaWorker extends App {
  val LOG = Logger.getLogger(getClass)
  //a. If worker running on a single host, spawn two workers
  // else spawn one worker.
  val nhosts = getPropertyOrElse("nhosts", 1)
  //Set the node, default to basic node
  val prop =
    getPropertyOrElse("node", "parabond.cluster.BasicNode")
  val clazz = Class.forName(prop)

  import parabond.cluster.Node

  val node = clazz.newInstance.asInstanceOf[Node]
  // One-port configuration
  val port1 = getPropertyOrElse("port", 8000)
  // If there is 1 host, then ports include 9000 by default
  // Otherwise, if there are two hosts in this configuration,
  // use just one port which must be specified by VM options
  val ports = if (nhosts == 1) List(port1, 9000) else List(port1)

  // will contain one port per host.
  for (port <- ports) {
    // Start up new worker.
    new ParaWorker(port)
  }
}

class ParaWorker(port: Int) extends Worker(port) {
  import ParaWorker._

  def act: Unit = {
    val name = getClass.getSimpleName
    LOG.info("started " + name + " (id=" + id + ")")

    def handlePartition(partition: Partition): Unit = {
      //Do calculations
      val analysis = node analyze(partition)

      var delta_t = 0L

      analysis.results.foreach(job => {
          delta_t += (job.result.t1 - job.result.t0)
        }
      )

      //Send result
      sendResult(delta_t)
    }

    def sendResult(delta_t: Long): Unit ={
      sender ! Result(delta_t, port)
    }

    while (true) {
      receive match {
        // TODO: Replace the code below to implement PNF
        // It gets the partition range info from the task payload then
        // spawns futures (or uses parallel collections) to analyze the
        // partition in parallel. Finally, when done, it replies
        // with the partial sum and the time elapsed time.
        case task: Task =>
          val payload = task.payload

          payload match {
            case payload: String => {
              sender ! "connection established!"
            }
            case payload: Partition => {
              LOG.info("received partition " + Partition)
              handlePartition(payload.asInstanceOf[Partition])
            }
          }
      }
    }
  }
}

package project.assign3

import org.apache.log4j.Logger
import parascale.actor.last.{Task, Worker}
import parascale.util._
import project.assign2.Partition
import project.assign2.Result

import scala.concurrent._
import ExecutionContext.Implicits.global


object ParaWorker extends App {
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
  val ports =
  if (nhosts == 1) List(port1, 9000) else List(port1)
  // will contain one port per host.
  for (port <- ports) {
    // Start up new worker.
    new ParaWorker(port)
  }
}

class ParaWorker(port: Int) extends Worker(port) {
  import ParaWorker._

  def act: Unit = {

  }
}

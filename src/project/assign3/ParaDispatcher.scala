package project.assign3

import org.apache.log4j.Logger
import parascale.actor.last.{Dispatcher, Task, Worker}
import parascale.util._
import project.assign2.Partition
import project.assign2.Result

import scala.concurrent._
import ExecutionContext.Implicits.global

object ParaDispatcher extends App {
  val socket2 = getPropertyOrElse("socket","localhost:9000")

  new ParaDispatcher(List("localhost:8000", socket2))
}

class ParaDispatcher(sockets: List[String])  extends Dispatcher(sockets) {
  import ParaDispatcher._

  def act: Unit = {
    val ladder = List(
      1000,
      2000,
      4000,
      8000,
      16000,
      32000,
      64000,
      100000
    )

    ladder.foreach(rung => {
      //To do: figure out how to import functions from ParaBond
      checkReset(rung, 0)

      val a = Partition(rung/2, 0)
      val b = Partition(rung/2, rung/2)


    })


  }
}
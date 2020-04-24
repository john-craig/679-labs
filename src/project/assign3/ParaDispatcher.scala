package project.assign3

import org.apache.log4j.Logger
import parascale.actor.last.{Dispatcher, Task, Worker}
import parascale.util._
import parabond.cluster._

import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.collection.mutable.ListBuffer

object ParaDispatcher extends App {
  val LOG = Logger.getLogger(getClass)
  val socket2 = getPropertyOrElse("socket","localhost:9000")

  new ParaDispatcher(List("localhost:8000", socket2))
}

class ParaDispatcher(sockets: List[String])  extends Dispatcher(sockets) {
  import ParaDispatcher._

  def act: Unit = {
    //Send connection messages to each Worker
    (0 until sockets.length).foreach { k =>
      LOG.info("sending message to worker " + k)
      workers(k) ! "to worker(" + k + ") hello from dispatcher"
    }

    //Define variables and constants
    val ladder = List(
      1000,
//      2000,
//      4000,
//      8000,
//      16000,
//      32000,
//      64000,
//      100000
    )

    var rungCounters = IndexedSeq(0, 0, 0)

    var alphaList = ListBuffer[Partition]()
    var bravoList = ListBuffer[Partition]()

    //Seperate the ladder into its partitions
    ladder.foreach(rung => {
      checkReset(rung, 0)

      val a = Partition(rung / 2, 0)
      val b = Partition(rung / 2, rung / 2)

      alphaList += a
      bravoList += b
    })

    val partitionIterators = List(alphaList.iterator, bravoList.iterator)

    //Output variables
    var totalRuntime = 0L
    var missedPortfIds = ListBuffer[List[Int]]()

    //Declare helper methods
    def handleResults(result: Result): Unit = {
      //Add the partial runtime to the total runtime
      totalRuntime += result.delta_t

      val missed = check(result.portfIds)
      LOG.info("missed portfolios: " + missed)
      missedPortfIds += missed

      //Determine the worker the result came from
      var worker = -1

      //Match the port
      (0 until workers.length).foreach(k => {
        if(sockets(k).contains(result.workerPort.toString)){
          worker = k
        }
      })

      rungCounters = rungCounters.updated(worker, rungCounters(worker) + 1)

      //Check the next rung, if available
      /*if(rungCounters(0) > rungCounters(2) & rungCounters(1) > rungCounters(2)){
        var portfIds = List[Int]()

        if(rungCounters(2) == 0){
          portfIds = (0 until ladder(rungCounters(2))).toList
        } else {
          portfIds = (ladder(rungCounters(2) - 1) until ladder(rungCounters(2))).toList
        }

        val missed = check(portfIds)
        LOG.info("missed portfolios: " + missed)
        missedPortfIds += missed

        rungCounters = rungCounters.updated(2, rungCounters(2) + 1)
      }*/

      //Send the next partition to the worker that just gave its result
      sendNextPartition(worker)
    }

    def sendNextPartition(worker: Int= -1): Unit ={
      //Match the port number to the worker, and send
      //that worker its next partitions
      if(worker == -1){
        //By default just send the next partition for both
        (0 until workers.length).foreach(k =>{
          workers(k) ! partitionIterators(k).next()
        })
      } else {
        if(partitionIterators(worker).hasNext) {
          workers(worker) ! partitionIterators(worker).next()
        }
      }
    }

    sendNextPartition()

    while (true) {
      receive match {
        case task: Task if task.kind == Task.REPLY =>
          val payload = task.payload

          LOG.info(task)

          payload match{
            case payload: String => {
              LOG.info(payload)
            }
            case payload: Result => {
              val result = payload.asInstanceOf[Result]

              //Check the results
              LOG.info("received result " + result)
              handleResults(result)
            }
          }

      }

    }
  }
}
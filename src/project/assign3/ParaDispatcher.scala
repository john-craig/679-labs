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

    //Define the ladder
    val ladder = List(
      1000,
      2000
//      4000,
//      8000,
//      16000,
//      32000,
//      64000,
//      100000
    )

    var alphaList = ListBuffer[Partition]()
    var bravoList = ListBuffer[Partition]()

    //Seperate the ladder into its partitions
    ladder.foreach(rung => {
      val a = Partition(rung / 2, 0)
      val b = Partition(rung / 2, rung / 2)

      alphaList += a
      bravoList += b
    })

    //val alphaIterator = alphaList.iterator
    //val bravoIterator = bravoList.iterator
    var partitionIterators = List(alphaList.iterator, bravoList.iterator)

    def sendNextPartition(port: Int= -1): Unit ={
      //Match the port number to the worker, and send
      //that worker its next partitions
      if(port == -1){
        //By default just send the next partition for both
        (0 until workers.length).foreach(k =>{
          workers(k) ! partitionIterators(k).next()
        })
      } else {
        //Match the port
        (0 until workers.length).foreach(k => {
          if(port.toString == sockets(k)){
            if(partitionIterators(k).hasNext){
              workers(k) ! partitionIterators(k).next()
            }
          }
        })
      }
    }

    while (true) {
      //Sends the partitions of the current candidate to the workers
      //then changes state so that it doesn't repeatedly send them over
      //and over; also increments the current candidate

      receive match {
        case task: Task if task.kind == Task.REPLY =>
          val payload = task.payload

          LOG.info(task)

          payload match{
            case payload: String => {
              LOG.info(payload)
              sendNextPartition()
            }
            case payload: Result => {
              val result = payload.asInstanceOf[Result]

              //Do whatever gubbins and shit needs done
              //when the results arrive

              //Send the next partition to the worker that just
              //sent a result
              sendNextPartition(result.workerPort)
            }
          }

      }

    }
  }
}
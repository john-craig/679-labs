package project.assign2

case class Partition(s: Long, e: Long, c: Long)
  extends Serializable {

  var start: Long = s
  var end: Long = e
  var candidate: Long = c

  def getRange(): Long ={
    return end - start
  }
}


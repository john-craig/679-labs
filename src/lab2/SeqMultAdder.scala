package lab2

object SeqMultAdder extends App {
  val nums = List(1, 3, 4, 5, 12, 2, 7, 9, 7)
  val odds = nums.filter {n => (n % 2)==1}
  val total = nums.foldLeft(0){(a,b) => (a + b + b)}

//  val f =  {n : Int => println(n)}

  def f(n: Int): Unit = println(n)

//  nums.foreach(println(_))
  odds.foreach(println(_))
  println(total)
}

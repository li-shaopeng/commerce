import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

object ts {
  def main(args: Array[String]): Unit = {
    val s = "aa\\nbb|ba"
    val strings: Array[String] = s.split("\\\\n")
    val tuples2: Array[(String, Int)] = strings.map(t =>(t,1))
    val a = strings
    val b: Array[String] = s.split("\\|")
    //    println(s)

    val hs = new Nothing
    hs.add("jack")
    hs.add("tom")
    hs.add("jack")
    hs.add("jack2")


  }
}

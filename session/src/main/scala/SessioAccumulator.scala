import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class SessioAccumulator extends AccumulatorV2[String, mutable.HashMap[String,Int]]{
  private var accHash = new mutable.HashMap[String, Int]()
  override def isZero: Boolean = {
    accHash.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String,Int]] = {
    val sessioAccumulator = new SessioAccumulator()
    accHash.synchronized{
      sessioAccumulator.accHash ++= this.accHash
    }
    sessioAccumulator
  }

  override def reset(): Unit = {
    accHash.clear()
  }

  override def add(v: String): Unit = {
    val i = this.accHash.getOrElse(v, 0)
    if(i == 0){
      accHash += (v -> i)
    }
    this.accHash.update(v, accHash(v) + 1)
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    /*other match {
      case acc:SessioAccumulator => {
        (this.accHash /: acc.value){ case (map, (k,v)) => map += ( k -> (v + map.getOrElse(k, 0)) )}
      }
    }*/
//    /*
    other.value.foldLeft(this.accHash){
      case (map,(key, v)) => {
        map += (key -> (map.getOrElse(key, 0) + v))
    }}
//    */
  }

  override def value: mutable.HashMap[String, Int] = {
    this.accHash
  }
}

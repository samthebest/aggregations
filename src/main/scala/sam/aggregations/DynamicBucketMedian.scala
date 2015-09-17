package sam.aggregations

import scala.collection.mutable

class DynamicBucketMedian(maxMapSize: Int) extends Median {
  // first implementation will use a mutable map, should be replaced with custom Array based implementation

  val m: mutable.Map[(Long, Long), Long] = mutable.Map.empty

  def update(e: Long): Unit = ???
  def result: Double = ???
  def update(m: Median): Unit = ???
}

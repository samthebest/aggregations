package sam.aggregations

import scala.collection.mutable

object DynamicBucketMedian {
  def addThenMerge(maxMapSize: Int, balanceRatio: Double, m: mutable.Map[(Long, Long), Long], e: Int): Unit = {

    // Something like:
    // find the center of mass
    // find the half way point (sort by values)
    // This will determine which side is unbalanced
    // Starting from the end point, find the first value that if merged it's size would not exceed the end-point
    // if no such point exists, then merge the end point

    // Alternate algo: when full, use max and min to half the number of points in the map by chopping into equal ranges
  }
}

class DynamicBucketMedian(maxMapSize: Int, balanceRatio: Double) extends Median {
  // first implementation will use a mutable map, should be replaced with custom Array based implementation

  val m: mutable.Map[(Long, Long), Long] = mutable.Map.empty

  def update(e: Long): Unit = ???
  def result: Double = ???
  def update(m: Median): Unit = ???
}

package sam.aggregations

import scala.collection.mutable

object DynamicBucketingMedian {
  def addThenMerge(maxMapSize: Int, balanceRatio: Double, m: mutable.Map[(Long, Long), Long], e: Int): Unit = {



  }

  // Simple algo that will merge the two adjacent buckets that will result in the smallest increase in range
  // I.e. merge the closest buckets
  def mergeClosestBuckets(m: mutable.Map[(Long, Long), Long]): Unit = ???

  // Could vary the above simply by using an "objective function" that includes how far away the buckets are from the
  // current medien.

  // Merge Algo 1
  // find the center of mass
  // find the half way point (sort by values)
  // This will determine which side is unbalanced
  // Starting from the end point, find the first value that if merged it's size would not exceed the end-point
  // if no such point exists, then merge the end point
}

case class DynamicBucketingMedian(maxMapSize: Int, balanceRatio: Double = 1.0) extends Median {
  // first implementation will use a mutable map, should be replaced with custom Array based implementation

  val m: mutable.Map[(Long, Long), Long] = mutable.Map.empty

  def update(e: Long): Unit = ???
  def result: Double = ???
  def update(m: Median): Unit = ???
}

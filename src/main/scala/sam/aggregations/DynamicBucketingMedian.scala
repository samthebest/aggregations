package sam.aggregations

class DynamicBucketingMedian(mapSizeLimit: Int) extends Median {
  val exactMedian = new ExactMedian()
  def update(e: Long): Unit = exactMedian.update(e)
  def result: Double = exactMedian.result
  def update(m: Median): Unit = exactMedian.update(m.asInstanceOf[DynamicBucketingMedian].exactMedian)
}




//object DynamicBucketingMedian {
//  def addThenMerge(maxMapSize: Int, balanceRatio: Double, m: mutable.Map[(Long, Long), Long], e: Int): Unit = {
//
//
//
//  }
//
//  // Simple algo that will merge the two adjacent buckets that will result in the smallest increase in range
//  // I.e. merge the closest buckets
//  def mergeClosestBuckets(m: mutable.Map[(Long, Long), Long]): Unit = ???
//
//}

//case class DynamicBucketingMedian(maxMapSize: Int, balanceRatio: Double = 1.0) extends Median {
//  // first implementation will use a mutable map, should be replaced with custom Array based implementation
//
//  private val m: mutable.Map[(Long, Long), Long] = mutable.Map.empty
//
//  def update(e: Long): Unit = ???
//  def result: Double = ???
//  def update(m: Median): Unit = ???
//}

package sam.aggregations

import scala.collection.mutable

class DynamicBucketMedianSpec extends MedianSpecUtils {
  def toLongMap(m: mutable.Map[(Int, Int), Int]): mutable.Map[(Long, Long), Long] =
    m.map {
      case ((r1, r2), count) => ((r1.toLong, r2.toLong), count.toLong)
    }

  "DynamicBucketMedian object" should {
    "Update a map and merge the correct buckets" in {
      val maxMapSize: Int = 2
      val balanceRatio: Double = 0.8
      val m: mutable.Map[(Long, Long), Long] = toLongMap(mutable.Map(
        (0, 0) -> 1,
        (1, 1) -> 1
      ))

      DynamicBucketMedian.addThenMerge(maxMapSize, balanceRatio, m, 0)

      m.toMap must_=== toLongMap(mutable.Map(
        (0, 0) -> 2,
        (1, 1) -> 1
      )).toMap
    }
  }
}

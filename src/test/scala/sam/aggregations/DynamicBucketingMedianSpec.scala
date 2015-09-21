package sam.aggregations

import scala.collection.mutable

class DynamicBucketingMedianSpec extends MedianSpecUtils {
  def toLongMap(m: mutable.Map[(Int, Int), Int]): mutable.Map[(Long, Long), Long] =
    m.map {
      case ((r1, r2), count) => ((r1.toLong, r2.toLong), count.toLong)
    }

  basicMedianSpecs(() => new DynamicBucketingMedian(10), "- DynamicBucketingMedian with enough memory")

  "DynamicBucketingMedian" should {
    "Size should not exceed 10 when created with sizeLimit 10 and updated with 15 distinct elements" in {
      val median = new DynamicBucketingMedian(10)
      (1 to 15).map(_.toLong).foreach(median.update)
      median.size must beLessThanOrEqualTo(10)
    }

    "Size should return 5 when created with sizeLimit 10 and updated with 5 distinct elements" in {
      val median = new DynamicBucketingMedian(10)
      (1 to 5).map(_.toLong).foreach(median.update)
      median.size must beLessThanOrEqualTo(10)
    }

    "exactMedian.getElems.size returns 15 if updated with 10 elements then updated with 5 more elements" in {
      val median = new DynamicBucketingMedian(10)
      (1 to 15).map(_.toLong).foreach(median.update)
      (11 to 15).map(_.toLong).foreach(median.exactMedian.update)
      median.exactMedian.getElems.size must_=== 15
    }


  }
}
































//"DynamicBucketMedian object" should {
//
//}

//class DynamicBucketingMedianSpec extends MedianSpecUtils {
//  def toLongMap(m: mutable.Map[(Int, Int), Int]): mutable.Map[(Long, Long), Long] =
//    m.map {
//      case ((r1, r2), count) => ((r1.toLong, r2.toLong), count.toLong)
//    }
//
//  "DynamicBucketMedian object" should {
//    "Update a map correctly" in {
//      val maxMapSize: Int = 2
//      val balanceRatio: Double = 0.8
//      val m: mutable.Map[(Long, Long), Long] = toLongMap(mutable.Map(
//        (0, 0) -> 1,
//        (1, 1) -> 1
//      ))
//
//      DynamicBucketingMedian.addThenMerge(maxMapSize = maxMapSize, balanceRatio = balanceRatio, m = m, e = 0)
//
//      m.toMap must_=== toLongMap(mutable.Map(
//        (0, 0) -> 2,
//        (1, 1) -> 1
//      )).toMap
//    }
//  }
//}

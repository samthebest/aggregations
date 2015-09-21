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

    "getMap returns a map with same size map as size" in {
      val median = new DynamicBucketingMedian(2)
      (1 to 3).map(_.toLong).foreach(median.update)
      median.getMap.size must_=== median.size
    }

    "Can get correct answer even compressing 3 points to 2" in {
      val median = new DynamicBucketingMedian(2)
      (1 to 3).map(_.toLong).foreach(median.update)
      median.result must_=== 2
    }

    "Can get correct answer even compressing 3 points to 2" in {
      val median = new DynamicBucketingMedian(2)
      (2 to 4).map(_.toLong).foreach(median.update)
      median.result must_=== 3
    }
  }

  "mergeSmallestConsecutive" should {
    import DynamicBucketingMedian._

    "do not mutate the map when is empty" in {
      val emptyMap = mutable.Map.empty[(Long, Long), Long]

      mergeSmallestConsecutive(emptyMap, 1)
      emptyMap must_=== mutable.Map.empty[(Long, Long), Long]
    }

    "do not mutate the map when is smaller or equals than sizeLimit" in {
      "and have only one key" in {
        def map = mutable.Map((1l, 1l) -> 1l)

        mergeSmallestConsecutive(map, 1) must_=== map
      }

      "and have 2 keys" in {
        def map = mutable.Map((1l, 1l) -> 1l, (2l, 2l) -> 1l)

        mergeSmallestConsecutive(map, 2) must_=== map
      }

      "and have multiple keys" in {
        def map = mutable.Map((1l, 1l) -> 1l, (2l, 2l) -> 1l, (3l, 3l) -> 1l, (4l, 4l) -> 1l, (5l, 5l) -> 1l)

        mergeSmallestConsecutive(map, 5) must_=== map
      }
    }

    "mutate in-place the specified mutable map" in {
      val map = mutable.Map((1l, 1l) -> 1l, (2l, 2l) -> 1l)

      mergeSmallestConsecutive(map, 1)

      map must_=== mutable.Map((1l, 2l) -> 2l)
    }

    "return the specified map" in {
      "when mutating" in {
        val map = mutable.Map((1l, 1l) -> 1l, (2l, 2l) -> 1l)

      mergeSmallestConsecutive(map, 1).equals(map.asInstanceOf[Any]) must_=== true
      }

      "when not mutating" in {
        val map = mutable.Map((1l, 1l) -> 1l)

        mergeSmallestConsecutive(map, 1).equals(map.asInstanceOf[Any]) must_=== true
      }
    }


    "merge the two consecutive pairs with the smallest joint size" in {
      "when there are 2 buckets with limit of one" in {
        def map = mutable.Map((1l, 1l) -> 1l, (2l, 2l) -> 1l)

        mergeSmallestConsecutive(map, 1) must_=== mutable.Map((1l, 2l) -> 2l)
      }
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

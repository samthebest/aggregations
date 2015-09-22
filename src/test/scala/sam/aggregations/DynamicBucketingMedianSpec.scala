package sam.aggregations

import sam.aggregations.DynamicBucketingMedian._

import scala.collection.mutable

class DynamicBucketingMedianSpec extends MedianSpecUtils {
  def toLongMap(m: mutable.Map[(Int, Int), Int]): mutable.Map[(Long, Long), Long] =
    m.map {
      case ((r1, r2), count) => ((r1.toLong, r2.toLong), count.toLong)
    }

  basicMedianSpecs(() => new DynamicBucketingMedian(10), "- DynamicBucketingMedian with enough memory")
  sufficientMemoryProperties(i => new DynamicBucketingMedian(i))

  "DynamicBucketingMedian" should {
    "Size should not exceed 1 when created with sizeLimit 1 and updated with 2 distinct elements" in {
      val median = new DynamicBucketingMedian(1)
      (1 to 2).map(_.toLong).foreach(median.update)
      median.size must beLessThanOrEqualTo(1)
    }

    "getMap correct when created with sizeLimit 1 and updated with 2 distinct elements" in {
      val median = new DynamicBucketingMedian(1)
      (1 to 2).map(_.toLong).foreach(median.update)
      median.getMap must_=== Map((1l, 2l) -> 2l)
    }

    "Size should not exceed 2 when created with sizeLimit 2 and updated with 3 distinct elements" in {
      val median = new DynamicBucketingMedian(2)
      (1 to 3).map(_.toLong).foreach(median.update)
      median.size must beLessThanOrEqualTo(2)
    }

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

    "getMap returns a map with same size map as size" in {
      val median = new DynamicBucketingMedian(2)
      (1 to 3).map(_.toLong).foreach(median.update)
      median.getMap.size must_=== median.size
    }

    "Can get correct map compressing 4 points to 2" in {
      val median = new DynamicBucketingMedian(2)
      (1 to 4).map(_.toLong).foreach(median.update)
      median.getMap must_=== Map((1l, 2l) -> 2l, (3l, 4l) -> 2l)
    }

    "Can get correct answer compressing 3 points to 2" in {
      val median = new DynamicBucketingMedian(2)
      (1 to 3).map(_.toLong).foreach(median.update)
      median.result must_=== 2.0
    }

    "Can get correct answer compressing 3 points to 2" in {
      val median = new DynamicBucketingMedian(2)
      (2 to 4).map(_.toLong).foreach(median.update)
      median.result must_=== 3.0
    }

    "Can get correct answer compressing 4 points to 2" in {
      val median = new DynamicBucketingMedian(2)
      (1 to 4).map(_.toLong).foreach(median.update)
      median.result must_=== 2.5
    }

    "Can get correct answer based on regression test 1" in {
      val median = new DynamicBucketingMedian(7)
      List(4l, 10l, 5l, 10l).foreach(median.update)
      median.result must_=== 7.5
    }

    "Can get correct map based on regression test 1" in {
      val median = new DynamicBucketingMedian(7)
      List(4l, 10l, 5l, 10l).foreach(median.update)
      median.getMap must_=== Map((4l, 4l) -> 1l, (10l, 10l) -> 2l, (5l, 5l) -> 1l)
    }
  }

  "Merge update" should {
    "work with trivial merging" in {
      val median = new DynamicBucketingMedian(1)
      median.update(1l)
      median.update(2l)

      val median2 = new DynamicBucketingMedian(1)
      median2.update(1l)
      median2.update(2l)

      median.update(median2)

      median.getMap must_=== Map((1l, 2l) -> 4l)
    }

    "Correctly merges identical buckets over close buckets and gives correct median" in {
      val median = new DynamicBucketingMedian(3)
      median.update(1l)
      median.update(2l)
      median.update(3l)
      median.update(4l)
      median.getMap must_=== Map((1l, 2l) -> 2l, (3l, 3l) -> 1l, (4l, 4l) -> 1l)

      val median2 = new DynamicBucketingMedian(1)
      median2.update(1l)
      median2.update(6l)
      median2.getMap must_=== Map((1l, 6l) -> 2l)

      median.update(median2)
      median.getMap must_=== Map((1l, 2l) -> 2l, (3l, 4l) -> 2l, (1l, 6l) -> 2l)

      median.update(median2)
      median.getMap must_=== Map((1l, 2l) -> 2l, (3l, 4l) -> 2l, (1l, 6l) -> 4l)

      median.result must_=== 3.0
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

    "return the specified mutable map" in {
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
      "when there are 2 buckets with limit of 1" in {
        def map = mutable.Map((1l, 1l) -> 1l, (2l, 2l) -> 1l)
        mergeSmallestConsecutive(map, 1) must_=== mutable.Map((1l, 2l) -> 2l)
      }

      "when there are 3 buckets with limit of 1" in {
        def map = mutable.Map((1l, 1l) -> 1l, (2l, 2l) -> 1l, (3l, 3l) -> 1l)
        mergeSmallestConsecutive(map, 1) must_=== mutable.Map((1l, 3l) -> 3l)
      }

      "when there are 3 buckets with limit of 2" in {
        def map = mutable.Map((1l, 1l) -> 1l, (2l, 2l) -> 1l, (3l, 3l) -> 1l)
        mergeSmallestConsecutive(map, 2) must_=== mutable.Map((1l, 2l) -> 2l, (3l, 3l) -> 1)
      }

      "when there are 7 unit buckets with limit of 1 with different counts but without gaps" in {
        def map = mutable.Map((1l, 1l) -> 1l, (2l, 2l) -> 10l, (3l, 3l) -> 9l, (4l, 4l) -> 5l, (5l, 5l) -> 24l,
          (6l, 6l) -> 1l, (7l, 7l) -> 18l)
        val sum = map.values.sum
        mergeSmallestConsecutive(map, 1) must_=== mutable.Map((1l, 7l) -> sum)
      }

      "when there are 7 unit buckets with limit of 4 with different counts but without gaps" in {
        def map = mutable.Map((1l, 1l) -> 1l, (2l, 2l) -> 10l, (3l, 3l) -> 9l,
          (4l, 4l) -> 5l, (5l, 5l) -> 24l, (6l, 6l) -> 1l, (7l, 7l) -> 18l)

        mergeSmallestConsecutive(map, 4) must_=== mutable.Map((1l, 2l) -> 11l, (3l, 4l) -> 14l,
          (5l, 6l) -> 25l, (7l, 7l) -> 18l)
      }

      "when there are 7 unit buckets with limit of 3 with different counts but without gaps" in {
        def map = mutable.Map((1l, 1l) -> 1l, (2l, 2l) -> 10l, (3l, 3l) -> 9l, (4l, 4l) -> 5l, (5l, 5l) -> 24l,
          (6l, 6l) -> 1l, (7l, 7l) -> 18l)

        mergeSmallestConsecutive(map, 3) must_=== mutable.Map((1l, 2l) -> 11l, (3l, 4l) -> 14l, (5l, 7l) -> 43l)
      }

      "when there are 7 unit buckets with limit of 3 with different counts and gaps" in {
        def map = mutable.Map((1l, 1l) -> 1l, (2l, 2l) -> 10l, (3l, 3l) -> 9l, (14l, 14l) -> 5l, (16l, 16l) -> 24l,
          (26l, 26l) -> 1l, (30l, 30l) -> 18l)

        mergeSmallestConsecutive(map, 3) must_=== mutable.Map((1l, 3l) -> 20l, (14l, 16l) -> 29l, (26l, 30l) -> 19l)
      }

      "when there are 7 buckets of different sizes with limit of 3 with different counts and gaps" in {
        def map = mutable.Map((1l, 2l) -> 11l, (3l, 3l) -> 9l, (14l, 16l) -> 29l, (22l, 22l) -> 3l,
          (26l, 26l) -> 1l, (30l, 32l) -> 18l)

        mergeSmallestConsecutive(map, 3) must_=== mutable.Map((1l, 3l) -> 20l, (14l, 16l) -> 29l, (22l, 32l) -> 22l)
      }

      "when 2 overlapping" in {
        def map = mutable.Map((1l, 2l) -> 11l, (2l, 3l) -> 9l)
        mergeSmallestConsecutive(map, 1) must_=== mutable.Map((1l, 3l) -> 20l)
      }

      "when 3 overlapping with limit 2" in {
        def map = mutable.Map((1l, 2l) -> 11l, (2l, 10l) -> 9l, (4l, 16l) -> 22l)
        mergeSmallestConsecutive(map, 2) must_=== mutable.Map((1l, 10l) -> 20l, (4l, 16l) -> 22l)
      }
    }
  }

  "mergeSmallestConsecutive" should {
    "Find the middle range with correct counts, with 1 element Map" in {
      medianFromBuckets(Map((0l, 0l) -> 1l)) must_=== 0.0
    }

    "Find the middle range with correct counts, with 3 element Map" in {
      medianFromBuckets(Map((0l, 0l) -> 1l, (1l, 1l) -> 1l, (2l, 2l) -> 1l)) must_=== 1.0
    }

    "Find the middle range with correct counts, with 3 element Map" in {
      medianFromBuckets(Map((0l, 0l) -> 1l, (1l, 1l) -> 1l, (2l, 2l) -> 3l)) must_=== 2.0
    }

    "Find the middle range with correct counts, with 3 element Map and sum is 6" in {
      medianFromBuckets(Map((0l, 0l) -> 1l, (1l, 1l) -> 2l, (2l, 2l) -> 3l)) must_=== 1.5
    }

    "Find the middle range with correct counts, with 3 element Map and sum is 4" in {
      medianFromBuckets(Map((0l, 0l) -> 1l, (1l, 1l) -> 2l, (2l, 2l) -> 1l)) must_=== 1.0
    }

    "Find the middle range with correct counts, with 2 element Map and total count is 3" in {
      medianFromBuckets(Map((0l, 0l) -> 1l, (1l, 1l) -> 2l)) must_=== 1.0
    }

    "Find the middle range with correct counts, with 3 element Map and different counts" in {
      medianFromBuckets(Map((0l, 0l) -> 10l, (1l, 1l) -> 30l, (2l, 2l) -> 21l)) must_=== 1.0
    }

    "Find the middle range with correct counts, with 2 element Map and same counts" in {
      medianFromBuckets(Map((1l, 2l) -> 2l, (3l, 4l) -> 2l)) must_=== 2.5
    }

    "return the mean value of median bucket when sum is even and the middle indexes fall between two buckets" in {
      medianFromBuckets(Map((1l, 2l) -> 10l, (10l, 15l) -> 30l, (50l, 60l) -> 40)) must_=== 32.5
    }

    "return the mean value of median bucket when sum is even and the middle indexes fall between two buckets, " +
      "extra bucket with last count 18" in {
      medianFromBuckets(Map((1l, 2l) -> 10l, (6l, 8l) -> 20l, (10l, 15l) -> 30l, (50l, 60l) -> 40,
        (100l, 100l) -> 18l)) must_=== 12.5
    }

    "return the mean value of median bucket when sum is even and the middle indexes fall between two buckets, " +
      "extra bucket with last count 19" in {
      medianFromBuckets(Map((1l, 2l) -> 10l, (6l, 8l) -> 20l, (10l, 15l) -> 30l, (50l, 60l) -> 40,
        (100l, 100l) -> 19l)) must_=== 15.0
    }

    "return the mean value of median bucket when sum is even and the middle indexes fall between two buckets, " +
      "extra bucket with last count 19" in {
      medianFromBuckets(Map((1l, 2l) -> 10l, (6l, 8l) -> 20l, (10l, 15l) -> 30l, (50l, 60l) -> 40,
        (100l, 100l) -> 20l)) must_=== 32.5
    }

    "return the mean value of median bucket when sum is even and the middle indexes fall between two buckets, " +
      "extra bucket with last count 20" in {
      medianFromBuckets(Map((1l, 2l) -> 10l, (6l, 8l) -> 20l, (10l, 15l) -> 30l, (50l, 60l) -> 40,
        (100l, 100l) -> 21l)) must_=== 50.0
    }

    "return the mean value of median bucket when sum is even and the middle indexes fall between two buckets, " +
      "extra bucket with last count 21" in {
      medianFromBuckets(Map((1l, 2l) -> 10l, (6l, 8l) -> 20l, (10l, 15l) -> 30l, (50l, 60l) -> 40,
        (100l, 100l) -> 22l)) must_=== 55.0
    }
  }

  "mergeOverlappingInfo" should {
    "Not merge 2 non overlapping buckets" in {
      mergeOverlappingInfo(List((1l, 4l) -> 2l, (5l, 8l) -> 3l)) must_=== List(
        (1l, 4l) -> (2l, None),
        (5l, 8l) -> (3l, None)
      )
    }

    "Merge 2 overlapping buckets" in {
      mergeOverlappingInfo(List((1l, 4l) -> 2l, (4l, 8l) -> 3l)) must_=== List(
        (1l, 8l) -> (5l, Some(Map((1l, 4l) -> 2l, (4l, 8l) -> 3l)))
      )
    }

    "Merge 3 overlapping buckets" in {
      mergeOverlappingInfo(List((1l, 4l) -> 2l, (4l, 8l) -> 3l, (7l, 10l) -> 6l)) must_=== List(
        (1l, 10l) -> (11l, Some(Map((1l, 4l) -> 2l, (4l, 8l) -> 3l, (7l, 10l) -> 6l)))
      )
    }

    "Merge complex case 1" in {
      mergeOverlappingInfo(List(
        (1l, 4l) -> 2l,
        (4l, 8l) -> 3l,
        (9l, 10l) -> 6l,
        (11l, 12l) -> 6l,
        (12l, 15l) -> 2l,
        (13l, 15l) -> 3l,
        (14l, 14l) -> 4l,
        (16l, 20l) -> 5l,
        (17l, 19l) -> 7l,
        (22l, 25l) -> 8l,
        (22l, 27l) -> 9l
      )) must_=== List(
        (1l, 8l) -> (5l, Some(Map((1l, 4l) -> 2l, (4l, 8l) -> 3l))),
        (9l, 10l) -> (6l, None),
        (11l, 15l) -> (15l, Some(Map((11l, 12l) -> 6l, (12l, 15l) -> 2l, (13l, 15l) -> 3l, (14l, 14l) -> 4l))),
        (16l, 20l) -> (12l, Some(Map((16l, 20l) -> 5l, (17l, 19l) -> 7l))),
        (22l, 27l) -> (17l, Some(Map((22l, 25l) -> 8l, (22l, 27l) -> 9l)))
      )
    }
  }
}

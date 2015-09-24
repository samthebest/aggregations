package sam.aggregations

import sam.aggregations.DynamicBucketingMedian._

import scala.collection.mutable
import scala.util.Try

class DynamicBucketingMedianSpec extends MedianSpecUtils {
  def toLongMap(m: mutable.Map[(Int, Int), Int]): mutable.Map[(Long, Long), Long] =
    m.map {
      case ((r1, r2), count) => ((r1.toLong, r2.toLong), count.toLong)
    }

      def roundTo5(d: Double): Double = math.floor(d * 100000) / 100000

  basicMedianSpecs(() => new DynamicBucketingMedian(10), "- DynamicBucketingMedian with enough memory")
  sufficientMemoryProperties(i => new DynamicBucketingMedian(i))

  "DynamicBucketingMedian" should {
    "Size should not exceed 1 when created with sizeLimit 1 and updated with 2 distinct elements" in {
      val median = new DynamicBucketingMedian(1)
      (1 to 2).map(_.toLong).foreach(median.update)
      median.size must beLessThanOrEqualTo(1)
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

    "Correctly merges two simple medians and gives correct estimation" in {
      val median = new DynamicBucketingMedian(3)
      median.update(1l, 2l, 3l, 4l)

      val median2 = new DynamicBucketingMedian(1)
      median2.update(1l, 6l)

      median.update(median2)
      median.update(median2)

      median.result must_=== 3.0
    }
  }

  "mergeSmallestConsecutive" should {
    import DynamicBucketingMedian._

    "do not mutate the map when is empty" in {
      val emptyMap = mutable.Map.empty[(Long, Long), Long]
      mergeBuckets(emptyMap, 1)
      emptyMap must_=== mutable.Map.empty[(Long, Long), Long]
    }

    "do not mutate the map when is smaller or equals than sizeLimit" in {
      "and have only one key" in {
        def map = mutable.Map((1l, 1l) -> 1l)
        mergeBuckets(map, 1) must_=== map
      }

      "and have 2 keys" in {
        def map = mutable.Map((1l, 1l) -> 1l, (2l, 2l) -> 1l)
        mergeBuckets(map, 2) must_=== map
      }

      "and have multiple keys" in {
        def map = mutable.Map((1l, 1l) -> 1l, (2l, 2l) -> 1l, (3l, 3l) -> 1l, (4l, 4l) -> 1l, (5l, 5l) -> 1l)
        mergeBuckets(map, 5) must_=== map
      }
    }

    "mutate in-place the specified mutable map" in {
      val map = mutable.Map((1l, 1l) -> 1l, (2l, 2l) -> 1l)
      mergeBuckets(map, 1)
      map must_=== mutable.Map((1l, 2l) -> 2l)
    }

    "return the specified mutable map" in {
      "when mutating" in {
        val map = mutable.Map((1l, 1l) -> 1l, (2l, 2l) -> 1l)
        mergeBuckets(map, 1).equals(map.asInstanceOf[Any]) must_=== true
      }

      "when not mutating" in {
        val map = mutable.Map((1l, 1l) -> 1l)
        mergeBuckets(map, 1).equals(map.asInstanceOf[Any]) must_=== true
      }
    }

    "merge the two consecutive pairs with the smallest joint size" in {
      "when there are 2 buckets with limit of 1" in {
        def map = mutable.Map((1l, 1l) -> 1l, (2l, 2l) -> 1l)
        mergeBuckets(map, 1) must_=== mutable.Map((1l, 2l) -> 2l)
      }

      "when there are 3 buckets with limit of 1" in {
        def map = mutable.Map((1l, 1l) -> 1l, (2l, 2l) -> 1l, (3l, 3l) -> 1l)
        mergeBuckets(map, 1) must_=== mutable.Map((1l, 3l) -> 3l)
      }

      "when there are 3 buckets with limit of 2" in {
        def map = mutable.Map((1l, 1l) -> 1l, (2l, 2l) -> 1l, (3l, 3l) -> 1l)
        mergeBuckets(map, 2) must_=== mutable.Map((1l, 2l) -> 2l, (3l, 3l) -> 1)
      }

      "when there are 7 unit buckets with limit of 1 with different counts but without gaps" in {
        def map = mutable.Map((1l, 1l) -> 1l, (2l, 2l) -> 10l, (3l, 3l) -> 9l, (4l, 4l) -> 5l, (5l, 5l) -> 24l,
          (6l, 6l) -> 1l, (7l, 7l) -> 18l)
        val sum = map.values.sum
        mergeBuckets(map, 1) must_=== mutable.Map((1l, 7l) -> sum)
      }

//      "when there are 7 unit buckets with limit of 4 with different counts but without gaps" in {
//        def map = mutable.Map((1l, 1l) -> 1l, (2l, 2l) -> 10l, (3l, 3l) -> 9l,
//          (4l, 4l) -> 5l, (5l, 5l) -> 24l, (6l, 6l) -> 1l, (7l, 7l) -> 18l)
//
//        mergeBuckets(map, 4) must_=== mutable.Map((1l, 2l) -> 11l, (3l, 4l) -> 14l,
//          (5l, 6l) -> 25l, (7l, 7l) -> 18l)
//      }

//      "when there are 7 unit buckets with limit of 3 with different counts but without gaps" in {
//        def map = mutable.Map((1l, 1l) -> 1l, (2l, 2l) -> 10l, (3l, 3l) -> 9l, (4l, 4l) -> 5l, (5l, 5l) -> 24l,
//          (6l, 6l) -> 1l, (7l, 7l) -> 18l)
//
//        mergeBuckets(map, 3) must_=== mutable.Map((1l, 2l) -> 11l, (3l, 4l) -> 14l, (5l, 7l) -> 43l)
//      }
//
//      "when there are 7 unit buckets with limit of 3 with different counts and gaps" in {
//        def map = mutable.Map((1l, 1l) -> 1l, (2l, 2l) -> 10l, (3l, 3l) -> 9l, (14l, 14l) -> 5l, (16l, 16l) -> 24l,
//          (26l, 26l) -> 1l, (30l, 30l) -> 18l)
//
//        mergeBuckets(map, 3) must_=== mutable.Map((1l, 3l) -> 20l, (14l, 16l) -> 29l, (26l, 30l) -> 19l)
//      }

      "when there are 7 buckets of different sizes with limit of 3 with different counts and gaps" in {
        def map = mutable.Map((1l, 2l) -> 11l, (3l, 3l) -> 9l, (14l, 16l) -> 29l, (22l, 22l) -> 3l,
          (26l, 26l) -> 1l, (30l, 32l) -> 18l)

        mergeBuckets(map, 3) must_=== mutable.Map((1l, 3l) -> 20l, (14l, 16l) -> 29l, (22l, 32l) -> 22l)
      }

      "when 2 overlapping" in {
        def map = mutable.Map((1l, 2l) -> 11l, (2l, 3l) -> 9l)
        mergeBuckets(map, 1) must_=== mutable.Map((1l, 3l) -> 20l)
      }

      "when 3 overlapping with limit 2" in {
        def map = mutable.Map((1l, 2l) -> 11l, (2l, 10l) -> 9l, (4l, 16l) -> 22l)
        mergeBuckets(map, 2) must_=== mutable.Map((1l, 10l) -> 20l, (4l, 16l) -> 22l)
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

    "return correct median when have overlapping symmetrical buckets" in {
      medianFromBuckets(Map((1l, 2l) -> 10l, (2l, 3l) -> 10l)) must_=== 2.0
    }

    // There is an excel sheet attached to http://10.65.12.238:8070/browse/SBPINSIGHTS-568 which helped me calculate
    // these
    "return correct median for really complicated overlapping case 1" in {
      medianFromBuckets(Map(
        //        (1l, 10l) -> 15l,
        //        (1l, 4l) -> 6l,
        (4l, 15l) -> 20l,
//        (6l, 10l) -> 10l,
        //        (10l, 12l) -> 40l,
        (13l, 15l) -> 5l
      )) must_=== (5 + 12) / 2.0
    }

    "return correct median for really complicated overlapping case 2" in {
      medianFromBuckets(Map(
        (1l, 10l) -> 15l,
        (1l, 4l) -> 6l,
        (4l, 15l) -> 20l,
        (6l, 10l) -> 10l,
        (10l, 12l) -> 40l,
        (13l, 15l) -> 5l
      )) must_=== 10.0
    }

    "return correct median for really complicated overlapping case 8" in {
      medianFromBuckets(Map(
        (1l, 10l) -> 15l,
        (1l, 4l) -> 20l,
        (4l, 15l) -> 20l,
        (6l, 10l) -> 20l,
        (10l, 12l) -> 20l,
        (13l, 15l) -> 15l
      )) must_=== 8.0
    }

//    "Special case" in {
//      medianFromBuckets(Map(
//        (1l, 1l) -> 35l,
////        (1l, 4l) -> 20l,
//        (4l, 4l) -> 20l,
//        (6l, 6l) -> 20l,
//        (10l, 10l) -> 20l,
//        (13l, 13l) -> 15l
//      )) must_=== 8.0
//    }

    "return correct median for really complicated overlapping case 3" in {
      medianFromBuckets(Map(
        (1l, 10l) -> 15l,
        (1l, 4l) -> 6l,
        (4l, 15l) -> 20l,
        (6l, 10l) -> 10l,
//        (10l, 12l) -> 40l,
        (13l, 15l) -> 5l
      )) must_=== 8.0
    }

    "return correct median for really complicated overlapping case 4" in {
      medianFromBuckets(Map(
//        (1l, 10l) -> 15l,
//        (1l, 4l) -> 6l,
        (4l, 15l) -> 20l,
        (6l, 10l) -> 10l,
        //        (10l, 12l) -> 40l,
        (13l, 15l) -> 5l
      )) must_=== 10.0
    }

    "return correct median for really complicated overlapping case 5" in {
      medianFromBuckets(Map(
                (1l, 10l) -> 15l,
                (1l, 4l) -> 6l,
//        (4l, 15l) -> 20l,
        (6l, 10l) -> 10l,
                (10l, 12l) -> 40l,
        (13l, 15l) -> 5l
      )) must_=== 10.0
    }

    "return correct median for really complicated overlapping case 6" in {
      medianFromBuckets(Map(
        (1l, 10l) -> 15l,
        (1l, 4l) -> 6l,
        //        (4l, 15l) -> 20l,
        (6l, 10l) -> 10l,
//        (10l, 12l) -> 40l,
        (13l, 15l) -> 5l
      )) must_=== 8.0
      // )) must_=== 7.0
    }

    "return correct median for really complicated overlapping case 7" in {
      medianFromBuckets(Map(
        (1l, 10l) -> 15l,
        (1l, 4l) -> 6l,
        //        (4l, 15l) -> 20l,
        (6l, 10l) -> 10l
        //        (10l, 12l) -> 40l,
//        (13l, 15l) -> 5l
      )) must_=== 6.0
    }
  }

  "disjointify" should {
    // TODO This isn't testing disjointify, it's testing countMapToDensity
    "Disjointify two simple overlapping ranges" in {
      disjointify(List(
        (4l, 15l) -> 20l,
        (13l, 15l) -> 5l
      ).flatMap(detachEndpoints))
      .groupBy(_._1).mapValues(_.map(_._2).sum).toList.sortBy(_._1._1) must_=== List(
        (4l, 4l) -> 2.5,
        (5l, 12l) -> 8 * 1.5,
        (13l, 13l) -> 3.5,
        (14l, 14l) -> 2.5,
        (15l, 15l) -> 4.5
      )
    }

    "Disjointify many overlapping ranges" in {
      disjointify(List(
        (1l, 10l) -> 15l,
        (1l, 4l) -> 6l,
        (4l, 15l) -> 20l,
        (6l, 10l) -> 10l,
        (10l, 12l) -> 40l,
        (13l, 15l) -> 5l
      ).flatMap(detachEndpoints))
      .groupBy(_._1).mapValues(_.map(_._2).sum).mapValues(roundTo5).toList.sortBy(_._1._1) must_=== List(
        (1l, 1l) -> 4.3,
        (2l, 3l) -> 4.59999,
        (4l, 4l) -> 5.8,
        (5l, 5l) -> 2.8,
        (6l, 6l) -> 5.4,
        (7l, 9l) -> 13.2,
        (10l, 10l) -> roundTo5(20.06666667),
        (11l, 11l) -> roundTo5(14.16666667),
        (12l, 12l) -> roundTo5(15.16666667),
        (13l, 13l) -> 3.5,
        (14l, 14l) -> 2.5,
        (15l, 15l) -> 4.5
      )
    }
  }

  "detachEndpoints" should {
    "Correctly detach trivial range with trivial count" in {
      detachEndpoints((1l, 1l) -> 1l) must_=== List((1l, 1l) -> 1.0)
    }

    "Correctly detach trivial range with count 2" in {
      detachEndpoints((1l, 1l) -> 2l) must_=== List((1l, 1l) -> 2.0)
    }

    "Correctly detach trivial range with count 3" in {
      detachEndpoints((1l, 1l) -> 3l) must_=== List((1l, 1l) -> 3.0)
    }

    "Correctly throws exception for impossible case 1" in {
      Try(detachEndpoints((1l, 2l) -> 1l)).isFailure must beTrue
    }

    "Correctly throws exception for impossible case 2" in {
      Try(detachEndpoints((1l, 5l) -> 1l)).isFailure must beTrue
    }

    "Correctly detach pair range with count 2" in {
      detachEndpoints((1l, 2l) -> 2l) must_=== List((1l, 1l) -> 1.0, (2l, 2l) -> 1.0)
    }

    "Correctly detach pair range with count 3" in {
      detachEndpoints((1l, 2l) -> 3l) must_=== List((1l, 1l) -> 1.5, (2l, 2l) -> 1.5)
    }

    "Correctly detach 3 range with count 2" in {
      detachEndpoints((1l, 3l) -> 2l) must_=== List((1l, 1l) -> 1.0, (2l, 2l) -> 0.0, (3l, 3l) -> 1.0)
    }

    "Correctly detach 3 range with count 5" in {
      detachEndpoints((1l, 3l) -> 5l) must_=== List((1l, 1l) -> 2.0, (2l, 2l) -> 1.0, (3l, 3l) -> 2.0)
    }

    "Correctly detach 3 range with count 8" in {
      detachEndpoints((1l, 3l) -> 8l) must_=== List((1l, 1l) -> 3.0, (2l, 2l) -> 2.0, (3l, 3l) -> 3.0)
    }

    "Correctly detach 4 range with count 2" in {
      detachEndpoints((1l, 4l) -> 2l) must_=== List((1l, 1l) -> 1.0, (2l, 3l) -> 0.0, (4l, 4l) -> 1.0)
    }

    "Correctly detach 4 range with count 5" in {
      detachEndpoints((1l, 4l) -> 5l) must_=== List((1l, 1l) -> (1.0 + 3.0 / 4), (2l, 3l) -> (2 * 3.0 / 4), (4l, 4l) -> (1.0 + 3.0 / 4))
    }
  }

  "mergeOverlappingInfo" should {
    "Not merge 2 non overlapping buckets" in {
      mergeOverlappingInfo(List((1l, 4l) -> 2l, (5l, 8l) -> 3l)) must_=== List(
        (1l, 4l) ->(2l, None),
        (5l, 8l) ->(3l, None)
      )
    }

    "Merge 2 overlapping buckets" in {
      mergeOverlappingInfo(List((1l, 4l) -> 2l, (4l, 8l) -> 3l)) must_=== List(
        (1l, 8l) ->(5l, Some(List((1l, 4l) -> 2l, (4l, 8l) -> 3l)))
      )
    }

    "Merge 3 overlapping buckets" in {
      mergeOverlappingInfo(List((1l, 4l) -> 2l, (4l, 8l) -> 3l, (7l, 10l) -> 6l)) must_=== List(
        (1l, 10l) ->(11l, Some(List((1l, 4l) -> 2l, (4l, 8l) -> 3l, (7l, 10l) -> 6l)))
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
        (1l, 8l) ->(5l, Some(List((1l, 4l) -> 2l, (4l, 8l) -> 3l))),
        (9l, 10l) ->(6l, None),
        (11l, 15l) ->(15l, Some(List((11l, 12l) -> 6l, (12l, 15l) -> 2l, (13l, 15l) -> 3l, (14l, 14l) -> 4l))),
        (16l, 20l) ->(12l, Some(List((16l, 20l) -> 5l, (17l, 19l) -> 7l))),
        (22l, 27l) ->(17l, Some(List((22l, 25l) -> 8l, (22l, 27l) -> 9l)))
      )
    }
  }

  "countMapToDensity" should {
    "Turn trivial map into density correctly" in {
      countMapToDensity(List((0l, 1l) -> 2l)) must_===
        List((0l, 0l) -> 1.0, (1l, 1l) -> 1.0)
    }

    "Turn simple pair into density correctly" in {
      countMapToDensity(List((0l, 1l) -> 2l, (1l, 2l) -> 2l)) must_===
        List((0l, 0l) -> 1.0, (1l, 1l) -> 2.0, (2l, 2l) -> 1.0)
    }

    "Turn spread out pair into density correctly" in {
      countMapToDensity(List((0l, 4l) -> 2l, (4l, 7l) -> 2l)) must_=== List(
        (0l, 0l) -> 1.0,
        (1l, 3l) -> 0.0,
        (4l, 4l) -> 2.0,
        (5l, 6l) -> 0.0,
        (7l, 7l) -> 1.0
      )
    }



    "Turn spread out pair into density correctly with greater than 2 counts" in {
      countMapToDensity(List((0l, 4l) -> 4l, (4l, 7l) -> 4l)).map(p => (p._1, roundTo5(p._2))) must_=== List(
        (0l, 0l) -> (1.0 + 2.0 / 5),
        (1l, 3l) -> roundTo5(0.0 + 2.0 * 3 / 5),
        (4l, 4l) -> (2.0 + 2.0 / 5 + 2.0 / 4),
        (5l, 6l) -> (0.0 + 2.0 * 2 / 4),
        (7l, 7l) -> (1.0 + 2.0 / 4)
      )
    }

    "Turn spread out pair into density correctly with simple double overlap" in {
      countMapToDensity(List((0l, 4l) -> 4l, (4l, 4l) -> 4l, (3l, 3l) -> 3l)).map(p => (p._1, roundTo5(p._2))) must_=== List(
        (0l, 0l) -> (1.0 + 2.0 / 5),
        (1l, 2l) -> roundTo5(0.0 + 2.0 * 2 / 5),
        (3l, 3l) -> roundTo5(3.0 + 2.0 / 5),
        (4l, 4l) -> (4.0 + 1.0 + 2.0 / 5)
      )
    }

    "Turn spread out pair into density correctly with greater than 2 counts and double overlap" in {
      countMapToDensity(List((0l, 4l) -> 4l, (4l, 7l) -> 4l, (3l, 3l) -> 3l)).map(p => (p._1, roundTo5(p._2))) must_=== List(
        (0l, 0l) -> (1.0 + 2.0 / 5),
        (1l, 2l) -> roundTo5(0.0 + 2.0 * 2 / 5),
        (3l, 3l) -> roundTo5(3.0 + 2.0 / 5),
        (4l, 4l) -> (2.0 + 2.0 / 5 + 2.0 / 4),
        (5l, 6l) -> (0.0 + 2.0 * 2 / 4),
        (7l, 7l) -> (1.0 + 2.0 / 4)
      )
    }
  }
}

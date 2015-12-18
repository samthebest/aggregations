package sam.aggregations

import sam.aggregations.MedianEstimator._

import scala.collection.mutable
import scala.util.Try

class MedianEstimatorSpec extends MedianSpecUtils {
  def toLongMap(m: mutable.Map[(Int, Int), Int]): mutable.Map[(Long, Long), Long] =
    m.map {
      case ((r1, r2), count) => ((r1.toLong, r2.toLong), count.toLong)
    }

  def roundTo5(d: Double): Double = math.floor(d * 100000) / 100000

//  basicMedianSpecs(() => new MedianEstimator(10), "- MedianEstimator with enough memory")
//  sufficientMemoryProperties(i => new MedianEstimator(i))

  "MedianEstimator" should {
    "Can get correct answer compressing 3 points to 2" in {
      val median = new MedianEstimator(2)
      val state = median.zero
      (1 to 3).map(_.toLong).foreach(median.mutate(state, _))
      median.result(state) must_=== 2.0
    }

    "Can get correct answer compressing 3 points to 2" in {
      val median = new MedianEstimator(2)
      val state = median.zero
      (2 to 4).map(_.toLong).foreach(median.mutate(state, _))
      median.result(state) must_=== 3.0
    }

    "Can get correct answer compressing 4 points to 2" in {
      val median = new MedianEstimator(2)
      val state = median.zero
      (1 to 4).map(_.toLong).foreach(median.mutate(state, _))
      median.result(state) must_=== 2.5
    }

    "Can get correct answer based on regression test 1" in {
      val median = new MedianEstimator(7)
      val state = median.zero
      List(4l, 10l, 5l, 10l).foreach(median.mutate(state, _))
      median.result(state) must_=== 7.5
    }

    "Correctly merges two simple medians and gives correct estimation" in {
      val median = new MedianEstimator(3)
      val state = median.zero
      median.mutate(state, 1l, 2l, 3l, 4l)

      val median2 = new MedianEstimator(1)
      val state2 = median.zero
      median2.mutate(state2, 1l, 6l)

      median.mutateAdd(state, state2)
      median.mutateAdd(state, state2)

      median.result(state) must_=== 3.0
    }
  }

  "medianFromBuckets" should {
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

    "return correct median for really complicated overlapping case 1" in {
      medianFromBuckets(Map(
        (4l, 15l) -> 20l,
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

    "return correct median for really complicated overlapping case 3" in {
      medianFromBuckets(Map(
        (1l, 10l) -> 15l,
        (1l, 4l) -> 6l,
        (4l, 15l) -> 20l,
        (6l, 10l) -> 10l,
        (13l, 15l) -> 5l
      )) must_=== 8.0
    }

    "return correct median for really complicated overlapping case 4" in {
      medianFromBuckets(Map(
        (4l, 15l) -> 20l,
        (6l, 10l) -> 10l,
        (13l, 15l) -> 5l
      )) must_=== 10.0
    }

    "return correct median for really complicated overlapping case 5" in {
      medianFromBuckets(Map(
        (1l, 10l) -> 15l,
        (1l, 4l) -> 6l,
        (6l, 10l) -> 10l,
        (10l, 12l) -> 40l,
        (13l, 15l) -> 5l
      )) must_=== 10.0
    }

    "return correct median for really complicated overlapping case 6" in {
      medianFromBuckets(Map(
        (1l, 10l) -> 15l,
        (1l, 4l) -> 6l,
        (6l, 10l) -> 10l,
        (13l, 15l) -> 5l
      )) must_=== 8.0
    }

    "return correct median for really complicated overlapping case 7" in {
      medianFromBuckets(Map(
        (1l, 10l) -> 15l,
        (1l, 4l) -> 6l,
        (6l, 10l) -> 10l
      )) must_=== 6.0
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

  "medianFromOverlap" should {
    "Handle edge case where we lose accuracy from floating point" in {
      medianFromOverlap(
        cumOverlapping = CumOverlapping(
          lower = 13113l,
          upper = 13228l,
          count = 13l,
          cumCount = 2932l,
          overlappingMap = List(((13113l, 13200l), 6l), ((13163l, 13163l), 1l), ((13200l, 13228l), 6l))),
        middleIndex = 2932l
      ) must_=== 13228.0
    }
  }
}

package sam.aggregations

import org.specs2.mutable.Specification
import sam.aggregations.TypeAliases._

import scala.collection.mutable
import CappedBinHistogram._

import scala.util.Try

class CappedBinHistogramSpec extends Specification {
  sequential

  // TODO Use clever matcher instead of this
  def roundTo5(d: Double): Double = math.floor(d * 100000) / 100000

  "CappedBinHistogram.result" should {
    "getMap returns a map with same size map as size" in {
      val hist = new CappedBinHistogram(2)
      (1 to 3).map(_.toLong).foreach(hist.update)
      hist.result.size must_=== hist.size
    }

    "Can get correct map compressing 4 points to 2" in {
      val hist = new CappedBinHistogram(2)
      (1 to 4).map(_.toLong).foreach(hist.update)
      hist.result must_=== Map((1l, 2l) -> 2l, (3l, 4l) -> 2l)
    }

    "Can get correct map when updated with 4 elements" in {
      val hist = new CappedBinHistogram(7)
      List(4l, 10l, 5l, 10l).foreach(hist.update)
      hist.result must_=== Map((4l, 4l) -> 1l, (10l, 10l) -> 2l, (5l, 5l) -> 1l)
    }
  }

  "CappedBinHistogram default merge strat" should {
    "work with trivial merging" in {
      val hist = new CappedBinHistogram(1)
      hist.update(1l, 2l)

      val hist2 = new CappedBinHistogram(1)
      hist2.update(1l, 2l)

      hist.update(hist2)

      hist.result must_=== Map((1l, 2l) -> 4l)
    }

    "Correctly merges identical buckets over other buckets" in {
      val hist = new CappedBinHistogram(3)
      hist.update(1l, 2l, 3l, 4l)
      hist.result must_=== Map((1l, 2l) -> 2l, (3l, 3l) -> 1l, (4l, 4l) -> 1l)

      val hist2 = new CappedBinHistogram(1)
      hist2.update(1l, 6l, 6l)
      hist2.result must_=== Map((1l, 6l) -> 3l)

      hist.update(hist2)
      hist.result must_=== Map((1l, 2l) -> 2l, (3l, 4l) -> 2l, (1l, 6l) -> 3l)

      hist.update(hist2)
      hist.result must_=== Map((1l, 2l) -> 2l, (3l, 4l) -> 2l, (1l, 6l) -> 6l)
    }
  }

  "merge strats" should {

    def testMergeMethod(strat: MergeStrategy): Unit = {
      "do not mutate the map when is empty" in {
        val emptyMap = mutable.Map.empty[(Long, Long), Long]
        strat(emptyMap, 1)
        emptyMap must_=== mutable.Map.empty[(Long, Long), Long]
      }

      "do not mutate the map when is smaller or equals than sizeLimit" in {
        "and have only one key" in {
          def map = mutable.Map((1l, 1l) -> 1l)
          strat(map, 1) must_=== map
        }

        "and have 2 keys" in {
          def map = mutable.Map((1l, 1l) -> 1l, (2l, 2l) -> 1l)
          strat(map, 2) must_=== map
        }

        "and have multiple keys" in {
          def map = mutable.Map((1l, 1l) -> 1l, (2l, 2l) -> 1l, (3l, 3l) -> 1l, (4l, 4l) -> 1l, (5l, 5l) -> 1l)
          strat(map, 5) must_=== map
        }
      }

      "mutate in-place the specified mutable map" in {
        val map = mutable.Map((1l, 1l) -> 1l, (2l, 2l) -> 1l)
        strat(map, 1)
        map must_=== mutable.Map((1l, 2l) -> 2l)
      }

      "return the specified mutable map" in {
        "when mutating" in {
          val map = mutable.Map((1l, 1l) -> 1l, (2l, 2l) -> 1l)
          strat(map, 1).equals(map.asInstanceOf[Any]) must_=== true
        }

        "when not mutating" in {
          val map = mutable.Map((1l, 1l) -> 1l)
          strat(map, 1).equals(map.asInstanceOf[Any]) must_=== true
        }
      }


      "merge the two consecutive pairs with the smallest joint size" in {
        "when there are 2 buckets with limit of 1" in {
          def map = mutable.Map((1l, 1l) -> 1l, (2l, 2l) -> 1l)
          strat(map, 1) must_=== mutable.Map((1l, 2l) -> 2l)
        }

        "when there are 3 buckets with limit of 1" in {
          def map = mutable.Map((1l, 1l) -> 1l, (2l, 2l) -> 1l, (3l, 3l) -> 1l)
          strat(map, 1) must_=== mutable.Map((1l, 3l) -> 3l)
        }

        "when there are 3 buckets with limit of 2" in {
          def map = mutable.Map((1l, 1l) -> 1l, (2l, 2l) -> 1l, (3l, 3l) -> 1l)
          strat(map, 2) must_=== mutable.Map((1l, 2l) -> 2l, (3l, 3l) -> 1)
        }

        "when there are 7 unit buckets with limit of 1 with different counts but without gaps" in {
          def map = mutable.Map((1l, 1l) -> 1l, (2l, 2l) -> 10l, (3l, 3l) -> 9l, (4l, 4l) -> 5l, (5l, 5l) -> 24l,
            (6l, 6l) -> 1l, (7l, 7l) -> 18l)
          val sum = map.values.sum
          strat(map, 1) must_=== mutable.Map((1l, 7l) -> sum)
        }

        "when there are 7 buckets of different sizes with limit of 3 with different counts and gaps" in {
          def map = mutable.Map((1l, 2l) -> 11l, (3l, 3l) -> 9l, (14l, 16l) -> 29l, (22l, 22l) -> 3l,
            (26l, 26l) -> 1l, (30l, 32l) -> 18l)
          strat(map, 3) must_=== mutable.Map((1l, 3l) -> 20l, (14l, 16l) -> 29l, (22l, 32l) -> 22l)
        }

        "when 2 overlapping" in {
          def map = mutable.Map((1l, 2l) -> 11l, (2l, 3l) -> 9l)
          strat(map, 1) must_=== mutable.Map((1l, 3l) -> 20l)
        }

        "when 3 overlapping with limit 2" in {
          def map = mutable.Map((1l, 2l) -> 11l, (2l, 10l) -> 9l, (4l, 16l) -> 22l)
          strat(map, 2) must_=== mutable.Map((1l, 10l) -> 20l, (4l, 16l) -> 22l)
        }
      }
    }

    testMergeMethod(mergeSmallestConsecutiveRange)
    testMergeMethod(mergeSmallestCountSum)

    "Merge smallest count sum correctly" in {
      def map = mutable.Map((1l, 1l) -> 10l, (2l, 2l) -> 10l, (3l, 30l) -> 1l, (31l, 50l) -> 1l)

      mergeSmallestCountSum(map, 3) must_=== mutable.Map((1l, 1l) -> 10l, (2l, 2l) -> 10l, (3l, 50l) -> 2l)
    }

    "when there are 7 unit buckets with limit of 4 with different counts but without gaps" in {
      def map = mutable.Map((1l, 1l) -> 1l, (2l, 2l) -> 10l, (3l, 3l) -> 9l,
        (4l, 4l) -> 5l, (5l, 5l) -> 24l, (6l, 6l) -> 1l, (7l, 7l) -> 18l)

      mergeSmallestConsecutiveRange(map, 4) must_=== mutable.Map((1l, 2l) -> 11l, (3l, 4l) -> 14l,
        (5l, 6l) -> 25l, (7l, 7l) -> 18l)
    }

    "when there are 7 unit buckets with limit of 3 with different counts but without gaps" in {
      def map = mutable.Map((1l, 1l) -> 1l, (2l, 2l) -> 10l, (3l, 3l) -> 9l, (4l, 4l) -> 5l, (5l, 5l) -> 24l,
        (6l, 6l) -> 1l, (7l, 7l) -> 18l)

      mergeSmallestConsecutiveRange(map, 3) must_=== mutable.Map((1l, 2l) -> 11l, (3l, 4l) -> 14l, (5l, 7l) -> 43l)
    }

    "when there are 7 unit buckets with limit of 3 with different counts and gaps" in {
      def map = mutable.Map((1l, 1l) -> 1l, (2l, 2l) -> 10l, (3l, 3l) -> 9l, (14l, 14l) -> 5l, (16l, 16l) -> 24l,
        (26l, 26l) -> 1l, (30l, 30l) -> 18l)

      mergeSmallestConsecutiveRange(map, 3) must_=== mutable.Map((1l, 3l) -> 20l, (14l, 16l) -> 29l, (26l, 30l) -> 19l)
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
      countMapToDensity(List((0l, 4l) -> 4l, (4l, 4l) -> 4l, (3l, 3l) -> 3l)).map(p => (p._1, roundTo5(p._2))) must_===
        List(
          (0l, 0l) -> (1.0 + 2.0 / 5),
          (1l, 2l) -> roundTo5(0.0 + 2.0 * 2 / 5),
          (3l, 3l) -> roundTo5(3.0 + 2.0 / 5),
          (4l, 4l) -> (4.0 + 1.0 + 2.0 / 5)
        )
    }

    "Turn spread out pair into density correctly with greater than 2 counts and double overlap" in {
      countMapToDensity(List((0l, 4l) -> 4l, (4l, 7l) -> 4l, (3l, 3l) -> 3l)).map(p => (p._1, roundTo5(p._2))) must_===
        List(
          (0l, 0l) -> (1.0 + 2.0 / 5),
          (1l, 2l) -> roundTo5(0.0 + 2.0 * 2 / 5),
          (3l, 3l) -> roundTo5(3.0 + 2.0 / 5),
          (4l, 4l) -> (2.0 + 2.0 / 5 + 2.0 / 4),
          (5l, 6l) -> (0.0 + 2.0 * 2 / 4),
          (7l, 7l) -> (1.0 + 2.0 / 4)
        )
    }

    "Disjointify two simple overlapping ranges" in {
      countMapToDensity(List(
        (4l, 15l) -> 20l,
        (13l, 15l) -> 5l
      )) must_=== List(
        (4l, 4l) -> 2.5,
        (5l, 12l) -> 8 * 1.5,
        (13l, 13l) -> 3.5,
        (14l, 14l) -> 2.5,
        (15l, 15l) -> 4.5
      )
    }

    "Disjointify many overlapping ranges" in {
      // TODO I should use some `closeTo` matcher or something rather than using dodgy floating points
      countMapToDensity(List(
        (1l, 10l) -> 15l,
        (1l, 4l) -> 6l,
        (4l, 15l) -> 20l,
        (6l, 10l) -> 10l,
        (10l, 12l) -> 40l,
        (13l, 15l) -> 5l
      )) must_=== List(
        (1l, 1l) -> 4.3,
        (2l, 3l) -> 4.6,
        (4l, 4l) -> 5.8,
        (5l, 5l) -> 2.8,
        (6l, 6l) -> 5.4,
        (7l, 9l) -> 13.200000000000001,
        (10l, 10l) -> 20.066666666666666,
        (11l, 11l) -> 14.166666666666666,
        (12l, 12l) -> 15.166666666666666,
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
      detachEndpoints((1l, 4l) -> 5l) must_===
        List((1l, 1l) -> (1.0 + 3.0 / 4), (2l, 3l) -> (2 * 3.0 / 4), (4l, 4l) -> (1.0 + 3.0 / 4))
    }
  }
}

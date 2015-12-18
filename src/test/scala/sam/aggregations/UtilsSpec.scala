package sam.aggregations

import org.specs2.mutable.Specification

import scala.util.{Failure, Success, Try}

class UtilsSpec extends Specification {
  "nthtileMapper" should {
    "Throw exception when n < 2" in {
      Try(Utils.nthTileMap[Long](1, Map.empty)) match {
        case s: Success[_] => failure("Expected IllegalArgumentException but got something: " + s)
        case Failure(ex: IllegalArgumentException) if ex.getMessage === "requirement failed: n must be greater than 1" => success
        case other => failure("Wrong kind of exception: " + other)
      }
    }

    "When distinct values is less than n, in this case 1 and n is 2, it throws an exception" in {
      Try(Utils.nthTileMap(
        n = 2,
        tToCount = Map((1L, 2L))
      )) match {
        case s: Success[_] => failure("Expected IllegalArgumentException but got something: " + s)
        case Failure(ex: IllegalArgumentException) if ex.getMessage === "requirement failed: Cannot define nth-tiles with less data " +
          "points (1) than n (2)" => success
        case other => failure("Wrong kind of exception: " + other)
      }
    }

    "When distinct values is less than n, in this case 2 and n is 3, it throws an exception" in {
      Try(Utils.nthTileMap(
        n = 3,
        tToCount = Map((1L, 2L), (2L, 1L))
      )) match {
        case s: Success[_] => failure("Expected IllegalArgumentException but got something: " + s)
        case Failure(ex: IllegalArgumentException) if ex.getMessage === "requirement failed: Cannot define nth-tiles with less data " +
          "points (2) than n (3)" => success
        case other => failure("Wrong kind of exception: " + other)
      }
    }

    "Correctly returns a mapper when n is 2 and just two data points" in {
      val mapper = Utils.nthTileMap(
        n = 2,
        tToCount = Map((5L, 1L), (20L, 1L))
      )

      mapper(5L) must_=== Some(0)
      mapper(20L) must_=== Some(1)
      mapper(15L) must_=== None
    }

    "Correctly returns a mapper when n is 2 and just two data points, where we allow non-strict bucketing" in {
      val mapper = Utils.nthTileMap(
        n = 2,
        tToCount = Map((5, 3L), (20, 1L))
      )

      mapper(5) must_=== Some(0)
      mapper(20) must_=== Some(1)
      mapper(15) must_=== None
    }


    // WRONG
    "We throw an exception when there does not exist a value that can take bucket n - 1" in {
      val mapper = Utils.nthTileMap(
        n = 2,
        tToCount = Map((5, 1L), (20, 2L))
      )

      mapper(5) must_=== Some(0)
      mapper(20) must_=== Some(0)
    }

    "We throw an exception when there does not exist a value that can take bucket n - 1" in {
      val mapper = Utils.nthTileMap(
        n = 3,
        tToCount = Map((5, 1L), (20, 1L), (33, 2L))
      )

      mapper(5) must_=== Some(0)
      mapper(20) must_=== Some(0)
      mapper(33) must_=== Some(1)
    }

    "Correctly returns a mapper when n is 2 and we have three distinct values" in {
      val mapper = Utils.nthTileMap(
        n = 2,
        tToCount = Map((5, 1L), (20, 3L), (22, 1L))
      )

      mapper(5) must_=== Some(0)
      mapper(20) must_=== Some(0)
      mapper(22) must_=== Some(1)
      mapper(15) must_=== None
    }

    "Returns correct quartiles given 4 values once" in {
      val mapper = Utils.nthTileMap(
        n = 4,
        tToCount = Map((5, 1L), (20, 1L), (22, 1L), (111, 1L))
      )

      mapper(5) must_=== Some(0)
      mapper(20) must_=== Some(1)
      mapper(22) must_=== Some(2)
      mapper(111) must_=== Some(3)
    }

    "Returns correct quartiles given values 1 to 8 once" in {
      val mapper = Utils.nthTileMap(
        n = 4,
        tToCount = (1 to 8).map((_, 1L)).toMap
      )

      mapper(1) must_=== Some(0)
      mapper(2) must_=== Some(0)
      mapper(3) must_=== Some(1)
      mapper(4) must_=== Some(1)
      mapper(5) must_=== Some(2)
      mapper(6) must_=== Some(2)
      mapper(7) must_=== Some(3)
      mapper(8) must_=== Some(3)
    }

    "Returns correct quartiles given values 1, 2, 2, 6, 6, 7, 7, 7" in {
      val mapper = Utils.nthTileMap(
        n = 4,
        tToCount = List(1, 2, 2, 6, 6, 6, 7, 7).groupBy(identity).mapValues(_.size)
      )

      mapper(1) must_=== Some(0)
      mapper(2) must_=== Some(0)
      mapper(6) must_=== Some(1)
      mapper(7) must_=== Some(3)
    }

    "Returns correct quartiles given values 1, 2, 2, 6, 6, 7, 7, 7, 8, 8, 8" in {
      val mapper = Utils.nthTileMap(
        n = 4,
        tToCount = List(1, 2, 2, 6, 6, 7, 7, 7, 7, 8, 8).groupBy(identity).mapValues(_.size)
      )

      mapper(1) must_=== Some(0)
      mapper(2) must_=== Some(0)
      mapper(6) must_=== Some(1)
      mapper(7) must_=== Some(1)
      mapper(8) must_=== Some(3)
    }

  }
}

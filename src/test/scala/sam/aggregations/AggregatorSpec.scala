package sam.aggregations

import org.specs2.mutable.Specification
import sam.aggregations.boiler_plate.AggToResultsCode._
import sam.aggregations.boiler_plate.MergeCode._
import sam.aggregations.boiler_plate.MutateCode._
import sam.aggregations.boiler_plate.ZerosCode._
import shapeless._
import HList._
import org.apache.spark.{SparkConf, SparkContext}

object StaticSparkContext {
  val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("Test Spark Engine"))
}

class AggregatorSpec extends Specification with Serializable {
  sequential

  "aggsToResults2" should {
    "Convert a state to a result" in {
      aggsToResults2(CountAggregator :: CountAggregator :: HNil, LongMutable(6L) :: LongMutable(9L) :: HNil) must_=== 6L :: 9L :: HNil
    }
  }

  "zeros1" should {
    "Return a zero correct" in {
      zeros1(CountAggregator :: HNil) must_=== LongMutable(0L) :: HNil
    }
  }

  "zeros2" should {
    "Return two zeros correct" in {
      zeros2(CountAggregator :: CountAggregator :: HNil) must_=== LongMutable(0L) :: LongMutable(0L) :: HNil
    }
  }

  import StaticSparkContext.sc

  "PimpedRDD.aggsByKey1" should {
    import Aggregator.PimpedPairRDD

    "Correctly count some strings" in {
      sc.makeRDD(Seq(1 -> "hello", 1 -> "world", 2 -> "is", 1 -> "fred", 2 -> "dude"))
      .aggByKey1(CountAggregator :: HNil).collect().toMap must_=== Map(
        1 -> (3L :: HNil),
        2 -> (2L :: HNil)
      )
    }

    "Correctly count some strings with two aggregators" in {
      sc.makeRDD(Seq(1 -> "hello", 1 -> "world", 2 -> "is", 1 -> "fred", 2 -> "dude"))
      .aggByKey2(CountAggregator :: CountAggregator :: HNil).collect().toMap must_=== Map(
        1 -> (3L :: 3L :: HNil),
        2 -> (2L :: 2L :: HNil)
      )
    }
  }

  "PimpedRDD.aggTree1" should {
    import Aggregator.PimpedPairRDD

    "Works when tree is trivially deep" in {
      sc.makeRDD(Seq("norwich" -> "hello", "norwich" -> "world", "london" -> "is", "norwich" -> "fred", "london" -> "dude"))
      .aggTree1(CountAggregator :: HNil, tree = Nil)
      .map(_.collect().toMap) must_=== List(Map(
        "norwich" -> (3L :: HNil),
        "london" -> (2L :: HNil)
      ))
    }

    "Works when tree is 1 levels deep" in {
      sc.makeRDD(Seq("norwich" -> "hello", "norwich" -> "world", "london" -> "is",
        "norwich" -> "fred", "london" -> "dude"))
      .aggTree1(
        aggregator = CountAggregator :: HNil,
        tree = List(Map("norwich" -> List("england"), "london" -> List("england")))
      )
      .map(_.collect().toMap) must_=== List(
        Map(
          "norwich" -> (3L :: HNil),
          "london" -> (2L :: HNil)
        ),
        Map(
          "england" -> (5L :: HNil)
        )
      )
    }

    "Works when tree is 2 levels deep" in {
      sc.makeRDD(Seq("norwich" -> "hello", "norwich" -> "world", "london" -> "is",
        "norwich" -> "fred", "london" -> "dude", "paris" -> "foo"))
      .aggTree1(
        aggregator = CountAggregator :: HNil,
        tree = List(
          Map("norwich" -> List("england"), "london" -> List("england"), "paris" -> List("france")),
          Map("england" -> List("europe"), "france" -> List("europe"))
        )
      )
      .map(_.collect().toMap) must_=== List(
        Map(
          "norwich" -> (3L :: HNil),
          "london" -> (2L :: HNil),
          "paris" -> (1L :: HNil)
        ),
        Map(
          "england" -> (5L :: HNil),
          "france" -> (1L :: HNil)
        ),
        Map(
          "europe" -> (6L :: HNil)
        )
      )
    }

    "Full interesting example using 3 step approach" in {
      trait Key
      case class MonthDemographic(code: Int, personType: String, monthSinceEpoch: Int) extends Key
      case class WindowDemographic(code: Int, personType: String, windowCenter: Int) extends Key
      case class WindowDemographicSimple(code: Int, windowCenter: Int) extends Key

      def monthDemographToThreeMonthWindows(key: Key): List[Key] = key match {
        case MonthDemographic(code, personType, month) =>
          (month - 1 to month + 1).map(WindowDemographic(code, personType, _)).toList
      }

      def windowToCodeParent(key: Key): Key = key match {
        case WindowDemographic(code, personType, window) => WindowDemographic(code - code % 100, personType, window)
      }

      def removePersonType(key: Key): Key = key match {
        case WindowDemographic(code, personType, window) => WindowDemographicSimple(code, window)
      }

      def removePersonTypeFromParentWindow(key: Key): List[Key] = key match {
        case WindowDemographicSimple(_, _) => Nil // signals early termination of tree (doesn't extend to complete depth)
        case WindowDemographic(code, personType, window) => List(WindowDemographicSimple(code, window))
      }

      val tree: List[Key => List[Key]] = List(
        monthDemographToThreeMonthWindows _,
        key => List(windowToCodeParent(key), removePersonType(key)),
        removePersonTypeFromParentWindow _
      )

      sc.makeRDD(Seq(
        MonthDemographic(1155, "priest", 5) -> "hello",
        MonthDemographic(1253, "priest", 7) -> "world",
        MonthDemographic(133, "friar", 6) -> "is",
        MonthDemographic(133, "friar", 6) -> "fred",
        MonthDemographic(1253, "friar", 12) -> "dude",
        MonthDemographic(133, "cardinal", 7) -> "dude",
        MonthDemographic(500, "cardinal", 13) -> "dude"
      ))
      .aggTree1(
        aggregator = CountAggregator :: HNil,
        tree = tree
      ).map(_.collect().toMap.mapValues(_.head)) must_=== List(
        Map(
          MonthDemographic(1155, "priest", 5) -> 1L,
          MonthDemographic(1253, "priest", 7) -> 1L,
          MonthDemographic(133, "friar", 6) -> 2L,
          MonthDemographic(1253, "friar", 12) -> 1L,
          MonthDemographic(133, "cardinal", 7) -> 1L,
          MonthDemographic(500, "cardinal", 13) -> 1L
        ),
        Map(
          WindowDemographic(1155, "priest", 4) -> 1L,
          WindowDemographic(1155, "priest", 5) -> 1L,
          WindowDemographic(1155, "priest", 6) -> 1L,

          WindowDemographic(1253, "priest", 6) -> 1L,
          WindowDemographic(1253, "priest", 7) -> 1L,
          WindowDemographic(1253, "priest", 8) -> 1L,

          WindowDemographic(133, "friar", 5) -> 2L,
          WindowDemographic(133, "friar", 6) -> 2L,
          WindowDemographic(133, "friar", 7) -> 2L,

          WindowDemographic(1253, "friar", 11) -> 1L,
          WindowDemographic(1253, "friar", 12) -> 1L,
          WindowDemographic(1253, "friar", 13) -> 1L,

          WindowDemographic(133, "cardinal", 6) -> 1L,
          WindowDemographic(133, "cardinal", 7) -> 1L,
          WindowDemographic(133, "cardinal", 8) -> 1L,

          WindowDemographic(500, "cardinal", 12) -> 1L,
          WindowDemographic(500, "cardinal", 13) -> 1L,
          WindowDemographic(500, "cardinal", 14) -> 1L
        ),
        Map(
          WindowDemographic(1100, "priest", 4) -> 1L,
          WindowDemographicSimple(1155, 4) -> 1L,
          WindowDemographic(1100, "priest", 5) -> 1L,
          WindowDemographicSimple(1155, 5) -> 1L,
          WindowDemographic(1100, "priest", 6) -> 1L,
          WindowDemographicSimple(1155, 6) -> 1L,

          WindowDemographic(1200, "priest", 6) -> 1L,
          WindowDemographicSimple(1253, 6) -> 1L,
          WindowDemographic(1200, "priest", 7) -> 1L,
          WindowDemographicSimple(1253, 7) -> 1L,
          WindowDemographic(1200, "priest", 8) -> 1L,
          WindowDemographicSimple(1253, 8) -> 1L,

          WindowDemographic(100, "friar", 5) -> 2L,
          WindowDemographicSimple(133, 5) -> 2L,
          WindowDemographic(100, "friar", 6) -> 2L,
          WindowDemographicSimple(133, 6) -> 3L,
          WindowDemographic(100, "friar", 7) -> 2L,
          WindowDemographicSimple(133, 7) -> 3L,

          WindowDemographic(1200, "friar", 11) -> 1L,
          WindowDemographicSimple(1253, 11) -> 1L,
          WindowDemographic(1200, "friar", 12) -> 1L,
          WindowDemographicSimple(1253, 12) -> 1L,
          WindowDemographic(1200, "friar", 13) -> 1L,
          WindowDemographicSimple(1253, 13) -> 1L,

          WindowDemographic(100, "cardinal", 6) -> 1L,
          WindowDemographic(100, "cardinal", 7) -> 1L,
          WindowDemographic(100, "cardinal", 8) -> 1L,
          WindowDemographicSimple(133, 8) -> 1L,

          WindowDemographic(500, "cardinal", 12) -> 1L,
          WindowDemographicSimple(500, 12) -> 1L,
          WindowDemographic(500, "cardinal", 13) -> 1L,
          WindowDemographicSimple(500, 13) -> 1L,
          WindowDemographic(500, "cardinal", 14) -> 1L,
          WindowDemographicSimple(500, 14) -> 1L

        ),
        Map(
          WindowDemographicSimple(1100, 4) -> 1L,
          WindowDemographicSimple(1100, 5) -> 1L,
          WindowDemographicSimple(1100, 6) -> 1L,

          WindowDemographicSimple(1200, 6) -> 1L,
          WindowDemographicSimple(1200, 7) -> 1L,
          WindowDemographicSimple(1200, 8) -> 1L,

          WindowDemographicSimple(100, 5) -> 2L,
          WindowDemographicSimple(100, 6) -> 3L,
          WindowDemographicSimple(100, 7) -> 3L,

          WindowDemographicSimple(1200, 11) -> 1L,
          WindowDemographicSimple(1200, 12) -> 1L,
          WindowDemographicSimple(1200, 13) -> 1L,

          WindowDemographicSimple(100, 8) -> 1L,

          WindowDemographicSimple(500, 12) -> 1L,
          WindowDemographicSimple(500, 13) -> 1L,
          WindowDemographicSimple(500, 14) -> 1L
        )
      )

    }
  }
}

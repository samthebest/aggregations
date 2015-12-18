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

class AggregatorSpec extends Specification {
  sequential

  val stringCounter = new CountAggregator[String]

  "aggsToResults2" should {
    "Convert a state to a result" in {
      aggsToResults2(stringCounter :: stringCounter :: HNil, LongMutable(6L) :: LongMutable(9L) :: HNil) must_=== 6L :: 9L :: HNil
    }
  }

  "zeros1" should {
    "Return a zero correct" in {
      zeros1(stringCounter :: HNil) must_=== LongMutable(0L) :: HNil
    }
  }

  "zeros2" should {
    "Return two zeros correct" in {
      zeros2(stringCounter :: stringCounter :: HNil) must_=== LongMutable(0L) :: LongMutable(0L) :: HNil
    }
  }

  import StaticSparkContext.sc

  "PimpedRDD.aggsByKey1" should {
    import Aggregator.PimpedPairRDD
    "Correctly count some strings" in {
      sc.makeRDD(Seq(1 -> "hello", 1 ->"world", 2 -> "is", 1 -> "fred", 2 -> "dude"))
      .aggByKey1(stringCounter :: HNil).collect().toMap must_=== Map(
        1 -> (3L :: HNil),
        2 -> (2L :: HNil)
      )
    }
  }

  "PimpedRDD.aggTree1" should {
    import Aggregator.PimpedPairRDD

    "Works when tree is trivially deep" in {
      sc.makeRDD(Seq("norwich" -> "hello", "norwich" ->"world", "london" -> "is", "norwich" -> "fred", "london" -> "dude"))
      .aggTree1(stringCounter :: HNil, tree = Nil).collect().toMap must_=== Map(
        "norwich" -> (3L :: HNil),
        "london" -> (2L :: HNil)
      )
    }

    "Works when tree is 1 levels deep" in {
      sc.makeRDD(Seq("norwich" -> "hello", "norwich" -> "world", "london" -> "is",
        "norwich" -> "fred", "london" -> "dude"))
      .aggTree1(stringCounter :: HNil,
        tree = List(Map("norwich" -> List("england")), Map("london" -> List("england"))))
      .collect().toMap must_=== Map(
        "norwich" -> (3L :: HNil),
        "london" -> (2L :: HNil)
      )
    }

    "Works when tree is 1 levels deep with composite keys" in {
      sc.makeRDD(Seq("norwich,tue" -> "hello", "norwich,tue" -> "world", "london,tue" -> "is",
        "norwich,thu" -> "fred", "london,thu" -> "dude"))
      .aggTree1(stringCounter :: HNil,
        tree = List(
          Map(
            "norwich,tue" -> List("england", "tue"),
            "norwich,thu" -> List("england", "thu")
          ),
          Map(
            "london,tue" -> List("england", "tue"),
            "london,thu" -> List("england", "thu")
          )
        ))
      .collect().toMap must_=== Map(
        "norwich" -> (3L :: HNil),
        "london" -> (2L :: HNil)
      )
    }

    "Works when tree is 1 levels deep with composite keys and +/- 1 day window" in {
      sc.makeRDD(Seq("norwich,tue" -> "hello", "norwich,tue" -> "world", "london,tue" -> "is",
        "norwich,thu" -> "fred", "london,thu" -> "dude"))
      .aggTree1(stringCounter :: HNil,
        tree = List(
          Map(
            "norwich,tue" -> List("england", "mon-wed", "tue-thu", "sun-tue"),
            "norwich,thu" -> List("england", "tue-thu", "wed-fri", "thu-sat")
          ),
          Map(
            "london,tue" -> List("england", "tue"),
            "london,thu" -> List("england", "thu")
          )
        ))
      .collect().toMap must_=== Map(
        "norwich" -> (3L :: HNil),
        "london" -> (2L :: HNil)
      )
    }

    "Full interesting example using 3 step approach" in {
      trait Key
      case class Id(id: Int) extends Key
      case class MonthDemographic(code: Int, personType: String, monthSinceEpoch: Int) extends Key
      case class WindowDemographic(code: Int, personType: String, windowCenter: Int) extends Key
      case class WindowDemographicSimple(code: Int, windowCenter: Int) extends Key

      val idToDemographicLookup: Map[Key, Key] = Map(
        Id(1) -> MonthDemographic(1155, "priest", 5),
        Id(2) -> MonthDemographic(1253, "priest", 7),
        Id(3) -> MonthDemographic(133, "friar", 6),
        Id(4) -> MonthDemographic(133, "friar", 6),
        Id(5) -> MonthDemographic(1253, "friar", 12),
        Id(6) -> MonthDemographic(133, "cardinal", 13),
        Id(7) -> MonthDemographic(500, "cardinal", 13)
      )

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
        id => List(idToDemographicLookup(id)),
        monthDemographToThreeMonthWindows _,
        key => List(windowToCodeParent(key), removePersonType(key)),
        removePersonTypeFromParentWindow _
      )

      sc.makeRDD(Seq(
        Id(1) -> "hello",
        Id(2) -> "world",
        Id(3) -> "is",
        Id(4) -> "fred",
        Id(5) -> "dude",
        Id(6) -> "dude",
        Id(7) -> "dude"
      ))
      .aggTree1(stringCounter :: HNil,
        tree = List(
          Map(
            "norwich,tue" -> List("england", "mon-wed", "tue-thu", "sun-tue"),
            "norwich,thu" -> List("england", "tue-thu", "wed-fri", "thu-sat")
          ),
          Map(
            "london,tue" -> List("england", "tue"),
            "london,thu" -> List("england", "thu")
          )
        ))
      .collect().toMap must_=== Map(
        "norwich" -> (3L :: HNil),
        "london" -> (2L :: HNil)
      )
    }

  }
}

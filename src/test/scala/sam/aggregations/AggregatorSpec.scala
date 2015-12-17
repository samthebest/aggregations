package sam.aggregations

import org.specs2.mutable.Specification

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
      Aggregator.aggsToResults2(stringCounter :: stringCounter :: HNil, LongMutable(6L) :: LongMutable(9L) :: HNil) must_=== 6L :: 9L :: HNil
    }
  }

  "zeros1" should {
    "Return a zero correct" in {
      Aggregator.zeros1(stringCounter :: HNil) must_=== LongMutable(0L) :: HNil
    }
  }

  "zeros2" should {
    "Return two zeros correct" in {
      Aggregator.zeros2(stringCounter :: stringCounter :: HNil) must_=== LongMutable(0L) :: LongMutable(0L) :: HNil
    }
  }

  import StaticSparkContext.sc

  "PimpedRDD.aggsByKey1" should {
    import Aggregator.PimpedPairRDD
    "Correctly count some strings" in {
      sc.makeRDD(Seq(1 -> "hello", 1 ->"world", 2 -> "is", 1 -> "fred", 2 -> "dude"))
      .aggsByKey1(stringCounter :: HNil).collect().toMap must_=== Map(
        1 -> (3L :: HNil),
        2 -> (2L :: HNil)
      )
    }
  }
}

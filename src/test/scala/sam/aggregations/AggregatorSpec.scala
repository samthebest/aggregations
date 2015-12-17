package sam.aggregations

import org.specs2.mutable.Specification

import shapeless._
import HList._

class AggregatorSpec extends Specification {
  sequential

  val stringCounter = new CountAggregator[String]

  "aggsToResults2" should {
    "Convert a state to a result" in {
      Aggregator.aggsToResults2(stringCounter :: stringCounter :: HNil, 6L :: 9L :: HNil) must_=== 6L :: 9L :: HNil
    }
  }

  "zeros1" should {
    "Return a zero correct" in {
      Aggregator.zeros1(stringCounter :: HNil) must_=== 0L :: HNil
    }
  }

  "zeros2" should {
    "Return two zeros correct" in {
      Aggregator.zeros2(stringCounter :: stringCounter :: HNil) must_=== 0L :: 0L :: HNil
    }
  }
}

package sam.aggregations

import org.specs2.mutable.Specification
import RangeUtils._

class RangeUtilsSpec extends Specification {
  sequential

  "rangesOverlap" should {
    "Return false for disjoint ranges (0, 4), (5, 7)" in {
      rangesOverlap((0, 4), (5, 7)) must beFalse
    }

    "Return true for overlapping ranges (0, 4), (4, 7)" in {
      rangesOverlap((0, 4), (4, 7)) must beTrue
    }

    "Return true for overlapping ranges (4, 7), (0, 4)" in {
      rangesOverlap((4, 7), (0, 4)) must beTrue
    }

    "Return true for overlapping ranges (0, 4), (0, 7)" in {
      rangesOverlap((0, 4), (0, 7)) must beTrue
    }

    "Return true for entirely covered range (1, 4), (0, 7)" in {
      rangesOverlap((1, 4), (0, 7)) must beTrue
    }

    "Return true for entirely covered range (0, 7), (1, 4)" in {
      rangesOverlap((0, 7), (1, 4)) must beTrue
    }
  }
}

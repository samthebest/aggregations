package sam.aggregations

import org.specs2.mutable.Specification
import Utils._

class RangeUtilsSpec extends Specification {
  sequential

  "rangesOverlap" should {
    "Return false for disjoint ranges (0, 4), (5, 7)" in {
      overlap((0, 4), (5, 7)) must beFalse
    }

    "Return true for overlapping ranges (0, 4), (4, 7)" in {
      overlap((0, 4), (4, 7)) must beTrue
    }

    "Return true for overlapping ranges (4, 7), (0, 4)" in {
      overlap((4, 7), (0, 4)) must beTrue
    }

    "Return true for overlapping ranges (0, 4), (0, 7)" in {
      overlap((0, 4), (0, 7)) must beTrue
    }

    "Return true for entirely covered range (1, 4), (0, 7)" in {
      overlap((1, 4), (0, 7)) must beTrue
    }

    "Return true for entirely covered range (0, 7), (1, 4)" in {
      overlap((0, 7), (1, 4)) must beTrue
    }

    "Return true for (7, 7), (7, 7)" in {
      overlap((7, 7), (7, 7)) must beTrue
    }

    "Return true for (7, 8), (7, 7)" in {
      overlap((7, 8), (7, 7)) must beTrue
    }

    "Return true for (1, 3), (3, 3)" in {
      overlap((1, 3), (3, 3)) must beTrue
    }

    "Return true for (3, 3), (1, 3)" in {
      overlap((3, 3), (1, 3)) must beTrue
    }
  }
}

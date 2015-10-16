package sam.aggregations

import org.specs2.mutable.Specification

class ExactHistogramSpec extends Specification {
  sequential

  "ExactHistogram" should {
    "Return an empty Map if not updated" in {
      val hist = Histogram[Long]()
      hist.result must_=== Map.empty
    }

    "Return correct map when updated with some interesting data" in {
      val hist = Histogram[Long]()
      hist.update(1L)
      hist.update(2L)
      hist.update(5L)
      hist.update(7L)
      hist.update(5L)
      hist.update(5L)
      hist.update(7L)
      hist.result must_=== Map(
        1L -> 1L,
        2L -> 1L,
        5L -> 3L,
        7L -> 2L
      )
    }

    "Return correct map when updated with another interesting map" in {
      val hist = Histogram[Long]()
      hist.update(1L)
      hist.update(2L)
      hist.update(5L)
      hist.update(7L)
      hist.update(5L)
      hist.update(5L)
      hist.update(7L)

      val hist2 = Histogram[Long]()
      hist2.update(1L)
      hist2.update(2L)
      hist2.update(5L)
      hist2.update(7L)
      hist2.update(5L)
      hist2.update(5L)
      hist2.update(7L)

      hist.update(hist2)

      hist.result must_=== Map(
        1L -> 2L,
        2L -> 2L,
        5L -> 6L,
        7L -> 4L
      )
    }

    
  }
}

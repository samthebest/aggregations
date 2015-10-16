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

    "Returns correct percentiles given values 1 to 100 once" in {
      val hist = Histogram[Long]()
      (1 to 100).map(_.toLong).foreach(hist.update)
      hist.percentiles.toList must_=== (1 to 100).map(_.toLong).toList
    }

    "Returns correct percentiles given values 1 to 100 twice" in {
      val hist = Histogram[Long]()
      (1 to 100).map(_.toLong).foreach(hist.update)
      (1 to 100).map(_.toLong).foreach(hist.update)
      hist.percentiles.toList must_=== (1 to 100).map(_.toLong).toList
    }

    "Returns correct percentiles given values 1 to 200" in {
      val hist = Histogram[Long]()
      (1 to 200).map(_.toLong).foreach(hist.update)
      hist.percentiles.toList must_=== (1 to 100).map(_.toLong * 2 - 1).toList
    }

    "Returns correct quartiles given values 1 to 4 once" in {
      val hist = Histogram[Long]()
      (1 to 4).map(_.toLong).foreach(hist.update)
      hist.nthtiles(4).toList must_=== (1 to 4).map(_.toLong).toList
    }

    "Returns correct quartiles given values 1 to 8 once" in {
      val hist = Histogram[Long]()
      (1 to 8).map(_.toLong).foreach(hist.update)
      hist.nthtiles(4).toList must_=== List(1, 3, 5, 7)
    }

    "Returns correct quartiles given values 1, 2, 2, 6, 6, 7, 7, 7" in {
      val hist = Histogram[Long]()
      List(1, 2, 2, 6, 6, 7, 7, 7).map(_.toLong).foreach(hist.update)
      hist.nthtiles(4).toList must_=== List(1, 2, 6, 7)
    }

    "Returns correct quartiles given values 1, 2, 2, 6, 6, 7, 7, 7, 8, 8, 8, 8" in {
      val hist = Histogram[Long]()
      List(1, 2, 2, 6, 6, 7, 7, 7, 8, 8, 8, 8).map(_.toLong).foreach(hist.update)
      hist.nthtiles(4).toList must_=== List(1, 6, 7, 8)
    }
  }
}

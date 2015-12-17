

package sam.aggregations

import org.specs2.mutable.Specification

import scala.collection.mutable

class CountHistogramSpec extends Specification {
  sequential

  "ExactHistogram" should {
    val hist = CountHistogram[Long]()
    "Return correct map when updated with some interesting data" in {
      val state = mutable.Map.empty[Long, Long]
      hist.mutate(state, 1L)
      hist.mutate(state, 2L)
      hist.mutate(state, 5L)
      hist.mutate(state, 7L)
      hist.mutate(state, 5L)
      hist.mutate(state, 5L)
      hist.mutate(state, 7L)
      hist.result(state) must_=== Map(
        1L -> 1L,
        2L -> 1L,
        5L -> 3L,
        7L -> 2L
      )
    }

    "Return correct map when updated with another interesting map" in {
      val state = mutable.Map.empty[Long, Long]
      hist.mutate(state, 1L)
      hist.mutate(state, 2L)
      hist.mutate(state, 5L)
      hist.mutate(state, 7L)
      hist.mutate(state, 5L)
      hist.mutate(state, 5L)
      hist.mutate(state, 7L)

      val state2 = mutable.Map.empty[Long, Long]
      hist.mutate(state2, 1L)
      hist.mutate(state2, 2L)
      hist.mutate(state2, 5L)
      hist.mutate(state2, 7L)
      hist.mutate(state2, 5L)
      hist.mutate(state2, 5L)
      hist.mutate(state2, 7L)

      hist.mutateAdd(state, state2)

      hist.result(state) must_=== Map(
        1L -> 2L,
        2L -> 2L,
        5L -> 6L,
        7L -> 4L
      )
    }

    "Returns correct percentiles given values 1 to 100 once" in {
      val state = mutable.Map.empty[Long, Long]
      (1 to 100).map(_.toLong).foreach(hist.mutate(state, _))
      hist.percentiles(state).toList must_=== (1 to 100).map(_.toLong).toList
    }

    "Returns correct percentiles given values 1 to 100 twice" in {
      val state = mutable.Map.empty[Long, Long]
      (1 to 100).map(_.toLong).foreach(hist.mutate(state, _))
      (1 to 100).map(_.toLong).foreach(hist.mutate(state, _))
      hist.percentiles(state).toList must_=== (1 to 100).map(_.toLong).toList
    }

    "Returns correct percentiles given values 1 to 200" in {
      val state = mutable.Map.empty[Long, Long]
      (1 to 200).map(_.toLong).foreach(hist.mutate(state, _))
      hist.percentiles(state).toList must_=== (1 to 100).map(_.toLong * 2 - 1).toList
    }

    "Returns correct quartiles given values 1 to 4 once" in {
      val state = mutable.Map.empty[Long, Long]
      (1 to 4).map(_.toLong).foreach(hist.mutate(state, _))
      hist.nthtiles(state)(4).toList must_=== (1 to 4).map(_.toLong).toList
    }

    "Returns correct quartiles given values 1 to 8 once" in {
      val state = mutable.Map.empty[Long, Long]
      (1 to 8).map(_.toLong).foreach(hist.mutate(state, _))
      hist.nthtiles(state)(4).toList must_=== List(1, 3, 5, 7)
    }

    "Returns correct quartiles given values 1, 2, 2, 6, 6, 7, 7, 7" in {
      val state = mutable.Map.empty[Long, Long]
      List(1, 2, 2, 6, 6, 7, 7, 7).map(_.toLong).foreach(hist.mutate(state, _))
      hist.nthtiles(state)(4).toList must_=== List(1, 2, 6, 7)
    }

    "Returns correct quartiles given values 1, 2, 2, 6, 6, 7, 7, 7, 8, 8, 8, 8" in {
      val state = mutable.Map.empty[Long, Long]
      List(1, 2, 2, 6, 6, 7, 7, 7, 8, 8, 8, 8).map(_.toLong).foreach(hist.mutate(state, _))
      hist.nthtiles(state)(4).toList must_=== List(1, 6, 7, 8)
    }
  }
}

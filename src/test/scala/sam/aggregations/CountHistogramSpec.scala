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
  }
}

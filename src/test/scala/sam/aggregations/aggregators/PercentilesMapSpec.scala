package sam.aggregations.aggregators

import org.specs2.mutable.Specification

import scala.collection.mutable

class PercentilesMapSpec extends Specification {
  "result" should {
    val percentileMapper = PercentilesMap[Long]()

    "Returns correct percentiles given values 1 to 100 once" in {
      val state = mutable.Map.empty[Long, Long]
      (1 to 100).map(_.toLong).foreach(percentileMapper.mutate(state, _))
      val map = percentileMapper.result(state)

      (1 to 100).map(i => map(i.toLong) must_=== Some(i - 1))

    }

    "Returns correct percentiles given values 1 to 100 twice" in {
      val state = mutable.Map.empty[Long, Long]
      (1 to 100).map(_.toLong).foreach(percentileMapper.mutate(state, _))
      (1 to 100).map(_.toLong).foreach(percentileMapper.mutate(state, _))
      val map = percentileMapper.result(state)
      (1 to 100).map(i => map(i.toLong) must_=== Some(i - 1))
    }

    "Returns correct percentiles given values 1 to 200" in {
      val state = mutable.Map.empty[Long, Long]
      (1 to 200).map(_.toLong).foreach(percentileMapper.mutate(state, _))

      percentileMapper.result(state)(1) must_=== Some(0)
      percentileMapper.result(state)(2) must_=== Some(0)
      percentileMapper.result(state)(3) must_=== Some(1)
      percentileMapper.result(state)(4) must_=== Some(1)
      percentileMapper.result(state)(197) must_=== Some(98)
      percentileMapper.result(state)(198) must_=== Some(98)
      percentileMapper.result(state)(199) must_=== Some(99)
      percentileMapper.result(state)(200) must_=== Some(99)
    }
  }
}

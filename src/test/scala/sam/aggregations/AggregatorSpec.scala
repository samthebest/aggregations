package sam.aggregations

import org.specs2.mutable.Specification

class AggregatorSpec extends Specification {
  sequential

  case class DummyAgg() extends Aggregator[Int, Int, DummyAgg] {
    override def update(e: Int): Unit = ???
    override def result: Int = ???
    override def update(a: DummyAgg): Unit = ???
  }

  "&" should {
    "correctly box two aggregators" in {
      (DummyAgg() & DummyAgg()) must_== &&[Int, DummyAgg, &&[Int, DummyAgg, MultiAggregatorNil[Int]]](
        DummyAgg(), &&[Int, DummyAgg, MultiAggregatorNil[Int]](DummyAgg(), MultiAggregatorNil[Int]()))
    }

    "correctly box three aggregators" in {
      ((DummyAgg() & DummyAgg()) & DummyAgg()) must_==
        &&[Int, DummyAgg, &&[Int, DummyAgg, &&[Int, DummyAgg, MultiAggregatorNil[Int]]]](DummyAgg(),
          &&(DummyAgg(), &&(DummyAgg(), MultiAggregatorNil[Int]())))
    }

    "correctly box three aggregators alternative bracketing" in {
      (DummyAgg() & (DummyAgg() & DummyAgg())) must_==
        &&[Int, DummyAgg, &&[Int, DummyAgg, &&[Int, DummyAgg, MultiAggregatorNil[Int]]]](DummyAgg(),
          &&(DummyAgg(), &&(DummyAgg(), MultiAggregatorNil[Int]())))
    }
  }

}

package sam.aggregations

import org.specs2.mutable.Specification

class AggregatorSpec extends Specification {
  sequential

  case class DummyAgg() extends AggregatorOps[Int, Int, DummyAgg] {
    override def update(e: Int): Unit = ???
    override def result: Int = ???
    override def update(a: DummyAgg): Unit = ???
  }

  "&" should {
    "correctly box two aggregators" in {
      (DummyAgg() & DummyAgg()) must_== &&[Int, DummyAgg, &&[Int, DummyAgg, MultiAggregatorOpsNil[Int]]](
        DummyAgg(), &&[Int, DummyAgg, MultiAggregatorOpsNil[Int]](DummyAgg(), MultiAggregatorOpsNil[Int]()))
    }

    "correctly box three aggregators" in {
      ((DummyAgg() & DummyAgg()) & DummyAgg()) must_==
        &&[Int, DummyAgg, &&[Int, DummyAgg, &&[Int, DummyAgg, MultiAggregatorOpsNil[Int]]]](DummyAgg(),
          &&(DummyAgg(), &&(DummyAgg(), MultiAggregatorOpsNil[Int]())))
    }

    "correctly box three aggregators alternative bracketing" in {
      (DummyAgg() & (DummyAgg() & DummyAgg())) must_==
        &&[Int, DummyAgg, &&[Int, DummyAgg, &&[Int, DummyAgg, MultiAggregatorOpsNil[Int]]]](DummyAgg(),
          &&(DummyAgg(), &&(DummyAgg(), MultiAggregatorOpsNil[Int]())))
    }
  }

  "alsoAgg" should {
    "correctly aggregate"
  }
}

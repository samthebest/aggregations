package sam.aggregations

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/** Aggregator useful for Map-Reduce programming.
  *
  * Unit return types is intentional and to highlight that it is not supposed to be used for pure functional
  * programming. This is because we wish to avoid memory allocation (and the consequent GC). */
trait Aggregator[+R, V, A <: Aggregator[R, V, A]] { self: A =>
  def update(e: V): Unit
  def update(a: A): Unit
  def result: R

  def +(e: V): A = {
    this.update(e)
    this
  }

  def +(a: A): A = {
    this.update(a)
    this
  }

  def update(e: V*): Unit = Seq(e: _*).foreach(update)

  def &[B <: Aggregator[_, V, B]](b: B): MultiAggregator[V] = b match {
    case ma: MultiAggregator[V] => &&[V, A, MultiAggregator[V]](this, ma)
    case _ => &&[V, A, MultiAggregator[V]](this, &&[V, B, MultiAggregator[V]](b, MultiAggregatorNil[V]()))
  }
}

trait MultiResult extends Product with Serializable

case object MultiResultNil extends MultiResult

final case class RR[+H, T <: MultiResult](head: H, tail: T) extends MultiResult

sealed trait MultiAggregator[V] extends Product with Serializable with Aggregator[MultiResult, V, MultiAggregator[V]] {
  // TODO We need to override & to make it so that the head is never a MutliAggregator - otherwise we will end up
  // with an unnecessary level of nesting.
}

case class MultiAggregatorNil[V]() extends MultiAggregator[V] {
  def update(e: V): Unit = ()
  def update(a: MultiAggregator[V]): Unit = ()
  def result: MultiResult = MultiResultNil
}

final case class &&[V, H <: Aggregator[_, V, H], T <: MultiAggregator[V]](head: H, tail: T) extends MultiAggregator[V] {
  def update(e: V): Unit = {
    head.update(e)
    tail.update(e)
  }

  def update(a: MultiAggregator[V]): Unit = a match {
    case &&(headA: H, tailA: T) =>
      head.update(headA)
      tail.update(tailA)
  }

  def result: MultiResult = RR(head.result, tail.result)
}

object Aggregator {
  implicit class PimpedRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) {
    def aggByKey[R, A <: Aggregator[R, V, A]](createAggregator: V => A): RDD[(K, A)] =
      rdd.combineByKey(createAggregator, _ + _, _ + _)
  }
}

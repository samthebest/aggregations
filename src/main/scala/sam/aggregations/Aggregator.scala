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

  def &[B <: Aggregator[_, V, B]](b: B) =
    &&[V, A, MultiAggregator[V]](this, &&[V, B, MultiAggregator[V]](b, MultiAggregatorNil[V]()))
}

// TODO Finish off this DSL - will be uber cool when finished.

// MultiAggregator should extend Aggregator, where the type of V is fixed, R is a "HList" (a MultiResult, no need to
// make shapeless a dependency for just one thing)

trait MultiResult extends Product with Serializable

case object MultiResultNil extends MultiResult

final case class RR[+H, T <: MultiResult](head: H, tail: T) extends MultiResult

sealed trait MultiAggregator[V] extends Product with Serializable with Aggregator[MultiResult, V, MultiAggregator[V]] {
  def update(e: V): Unit = ???
  def update(a: MultiAggregator[V]): Unit = ???
  def result: MultiResult = ???
}

case object MultiAggregatorNil extends MultiAggregator[Nothing]

case class MultiAggregatorNil[V]() extends MultiAggregator[V] {
  override def update(e: V): Unit = ()
  override def update(a: MultiAggregator[V]): Unit = ()
  override def result: MultiResult = MultiResultNil
}

final case class &&[V, H <: Aggregator[_, V, H], T <: MultiAggregator[V]](head: H, tail: T) extends MultiAggregator[V]

object Aggregator {
  implicit class PimpedRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) {
    def aggregateWith[R, A <: Aggregator[R, V, A]](createAggregator: V => A): RDD[(K, A)] =
      rdd.combineByKey(createAggregator, _ + _, _ + _)
  }
}

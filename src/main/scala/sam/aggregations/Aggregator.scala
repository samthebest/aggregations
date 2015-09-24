package sam.aggregations

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/** Aggregator useful for Map-Reduce programming.
  *
  * Unit return types is intentional and to highlight that it is not supposed to be used for pure functional
  * programming. This is because we wish to avoid memory allocation (and the consequent GC). */
trait Aggregator[+R, -V, A <: Aggregator[R, V, A]] { self: A =>
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
}

// TODO Finish off this DSL - will be uber cool when finished.
sealed trait MultiAggregator extends Product with Serializable

final case class &&[H <: Aggregator[_, _, H], T <: MultiAggregator](head: H, tail: T)

object Aggregator {
  // TODO To avoid the createAggregator argument, we could use ad-hoc polymorphism to introduce a default
  // type class that can be used to create the instance - it's then up to the user to create the type-class and
  // ensure it's in scope.  We can then just specify the type of the thing we wish to aggregate!!
  implicit class PimpedRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) {
    def aggregateWith[R, A <: Aggregator[R, V, A]](createAggregator: V => A): RDD[(K, A)] =
      rdd.combineByKey(createAggregator, _ + _, _ + _)
  }
}

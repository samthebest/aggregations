package sam.aggregations

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/** Aggregator useful for Map-Reduce programming.
  *
  * Unit return types is intentional and to highlight that it is not supposed to be used for pure functional
  * programming. This is because we wish to avoid memory allocation (and the consequent GC). */
trait Aggregator[R, V, T <: Aggregator[R, V, T]] { self: T =>
  def update(e: V): Unit
  def update(m: T): Unit
  def result: R

  def +(e: V): T = {
    this.update(e)
    this
  }

  def +(e: T): T = {
    this.update(e)
    this
  }
}

object Aggregator {
  implicit class PimpedRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) {
    def aggregateWith[R, A <: Aggregator[R, V, A]](createAggregator: V => A): RDD[(K, A)] =
      rdd.combineByKey(createAggregator, _ + _, _ + _)
  }
}

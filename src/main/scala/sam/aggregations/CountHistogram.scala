package sam.aggregations

import scala.collection.mutable
import scala.reflect.ClassTag

case class CountHistogram[T: ClassTag]() extends Aggregator[Map[T, Long], T, CountHistogram[T]] {
  private val m: mutable.Map[T, Long] = mutable.Map.empty

  def update(e: T): Unit = m.update(e, m.getOrElse(e, 0L) + 1)
  def result: Map[T, Long] = m.toMap
  def update(a: CountHistogram[T]): Unit = a.result.foreach {
    case (key, value) => m.update(key, m.getOrElse(key, 0L) + value)
  }

  def percentiles(implicit ordering: Ordering[T]): Array[T] = Utils.percentiles(result.toList)
  def nthtiles(n: Int)(implicit ordering: Ordering[T]): Array[T] = Utils.nthtiles(n, result.toList)
}

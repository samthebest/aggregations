package sam.aggregations

import scala.collection.mutable

case class Histogram[T]() extends Aggregator[Map[T, Long], T, Histogram[T]] {
  private val m: mutable.Map[T, Long] = mutable.Map.empty

  def update(e: T): Unit = m.update(e, m.getOrElse(e, 0L) + 1)
  def result: Map[T, Long] = m.toMap
  def update(a: Histogram[T]): Unit = a.result.foreach {
    case (key, value) => m.update(key, m.getOrElse(key, 0L) + value)
  }
}

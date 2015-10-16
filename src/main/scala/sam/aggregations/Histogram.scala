package sam.aggregations

import scala.collection.mutable
import scala.reflect.ClassTag

case class Histogram[T: ClassTag]() extends Aggregator[Map[T, Long], T, Histogram[T]] {
  private val m: mutable.Map[T, Long] = mutable.Map.empty

  def update(e: T): Unit = m.update(e, m.getOrElse(e, 0L) + 1)
  def result: Map[T, Long] = m.toMap
  def update(a: Histogram[T]): Unit = a.result.foreach {
    case (key, value) => m.update(key, m.getOrElse(key, 0L) + value)
  }

  // TODO Move all these into Utils then just have wrappers for convenience
  def cumulativeDensity(m: List[(T, Long)]): List[(T, Long)] =
    m.map(_._1).zip(m.drop(1).scanLeft(m.head._2)((cum, cur) => cum + cur._2))

  def percentiles(implicit ordering: Ordering[T]): Array[T] = nthtiles(100)

  def nthtiles(n: Int)(implicit ordering: Ordering[T]): Array[T] = {
    val cumCounts = cumulativeDensity(result.toList.sortBy(_._1))
    val total = cumCounts.last._2
    val percentileSize = total.toDouble / n

    cumCounts.foldLeft((Nil: List[T], 0)) {
      case (cum@(cumPercentiles, curPercentile), (t, count)) =>
        if (count > percentileSize * curPercentile) (t +: cumPercentiles, curPercentile + 1)
        else cum
    }
    ._1.reverse.toArray
  }
}

package sam.aggregations

import scala.reflect.ClassTag

object Utils {
  def overlap[T : Numeric](r1: (T, T), r2: (T, T)): Boolean = {
    val num = implicitly[Numeric[T]]
    import num.mkOrderingOps

    (r1, r2) match {
      case ((lower1, upper1), (lower2, _)) if lower1 < lower2 => upper1 >= lower2
      case ((lower1, _), (lower2, upper2)) if lower1 > lower2 => upper2 >= lower1
      case _ => true
    }
  }

  def cumulativeDensity[T](m: List[(T, Long)]): List[(T, Long)] =
    m.map(_._1).zip(m.drop(1).scanLeft(m.head._2)((cum, cur) => cum + cur._2))

  def percentiles[T: ClassTag](l: List[(T, Long)])(implicit ordering: Ordering[T]): Array[T] = nthtiles(100, l)

  // TODO Unit tests for case when total < n
  // TODO Think about case when num cumCounts.size < n, unit test
  def nthtiles[T: ClassTag](n: Int, l: List[(T, Long)])(implicit ordering: Ordering[T]): Array[T] = {
    val cumCounts = cumulativeDensity(l.sortBy(_._1))
    val total = cumCounts.last._2
    require(total >= n, "nthtiles don't make sense if we have less data points than n")
    val percentileSize = total.toDouble / n

    cumCounts.foldLeft((Nil: List[T], 0)) {
      case (cum@(cumPercentiles, curPercentile), (t, count)) =>
        if (count > percentileSize * curPercentile) (t +: cumPercentiles, curPercentile + 1)
        else cum
    }
    ._1.reverse.toArray
  }
}

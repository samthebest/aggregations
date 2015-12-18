package sam.aggregations

import scala.collection.immutable.IndexedSeq
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

  /** The resulting map will tell us the numerator X of the highest fraction X/n such that X/n or more values fall
    * strictly below the value passed to the map and X is an integer. If there is no such value T such that would return
    * n - 1, then we throw an exception since the given value of n is too large to make a n-level distinction
    * between the values */
  def nthTileMap[T](n: Int, tToCount: Map[T, Long])(implicit ordering: Ordering[T]): T => Option[Int] = {
    require(n > 1, "n must be greater than 1")
    require(n <= tToCount.size, s"Cannot define nth-tiles with less data points (${tToCount.size}) than n ($n)")

    val sorted: List[(T, Long)] = tToCount.toList.sortBy(_._1)
    val cumDensity: List[(T, Long)] = cumulativeDensityInclusive(sorted)
    val numValuesLessThanMap: List[(T, Long)] = sorted.map(_._1).zip(0L +: cumDensity.map(_._2).dropRight(1))
    val total: Long = cumDensity.last._2

    val numeratorToFraction: List[(Int, Double)] = (0 to n - 1).reverse.map(i => (i, i.toDouble * total / n)).toList

    (i: T) =>
      numValuesLessThanMap.toMap.get(i)
      .flatMap(numValuesLessThan =>
        // Probably inefficient, should be arithmetic trick, or should at least form a Map so we are memoized
        numeratorToFraction.find(_._2 <= numValuesLessThan.toDouble).map(_._1))
  }

  def cumulativeDensityInclusive[T](tToCount: List[(T, Long)]): List[(T, Long)] =
    tToCount.map(_._1).zip(tToCount.drop(1).scanLeft(tToCount.head._2)((cum, cur) => cum + cur._2))

  def percentilesMap[T: ClassTag](tToCount: Map[T, Long])(implicit ordering: Ordering[T]): T => Option[Int] =
    nthTileMap(100, tToCount)
}
package sam.aggregations

import scala.reflect.ClassTag

object Utils {
  /** The resulting map will tell us the numerator X of the highest fraction X/n such that X/n or more values fall
    * strictly below the value passed to the map and X is an integer. If there is no such value T such that would return
    * n - 1, then we throw an exception since the given value of n is too large to make a n-level distinction
    * between the values */
  def nthTileMap[T](n: Int, tToCount: Map[T, Long])(implicit ordering: Ordering[T]): T => Option[Int] = {
    require(n > 1, "n must be greater than 1")
    require(n <= tToCount.size, s"Cannot define nth-tiles with less data points (${tToCount.size}) than n ($n)")

    val sorted = tToCount.toList.sortBy(_._1)
    val cumDensity = cumulativeDensityInclusive(sorted)
    val total = cumDensity.last._2

    (i: T) =>
      sorted.map(_._1).zip(0L +: cumDensity.map(_._2).dropRight(1))
      .toMap.get(i).flatMap(numValuesLessThan =>
        // Probably inefficient, should be arithmetic trick, or should at least form a Map so we are memoized
        (0 to n - 1).reverse.find(_.toDouble * total / n <= numValuesLessThan.toDouble))
  }

  def cumulativeDensityInclusive[T](tToCount: List[(T, Long)]): List[(T, Long)] =
    tToCount.map(_._1).zip(tToCount.drop(1).scanLeft(tToCount.head._2)((cum, cur) => cum + cur._2))

  def percentilesMap[T: ClassTag](tToCount: Map[T, Long])(implicit ordering: Ordering[T]): T => Option[Int] =
    nthTileMap(100, tToCount)
}
package sam.aggregations

import scala.collection.immutable.Nil
import scala.reflect.ClassTag

object Utils {
  def nthTileMapper[T](n: Int, tToCount: Map[T, Long]) = {
    require(n > 1, "n must be greater than 1")
    require(n <= tToCount.size, s"Cannot define nth-tiles with less data points (${tToCount.size}) than n ($n)")
  }

  def cumulativeDensity[T](tToCount: List[(T, Long)]): List[(T, Long)] =
    tToCount.map(_._1).zip(tToCount.drop(1).scanLeft(tToCount.head._2)((cum, cur) => cum + cur._2))

//  def percentilesMapper[T: ClassTag](l: List[(T, Long)])(implicit ordering: Ordering[T]): T => Int = nthtilesMapper(100, l)

  // TODO Unit tests for case when total < n
  // TODO Think about case when num cumCounts.size < n, unit test
//  def nthtilesMapper[T: ClassTag](n: Int, tToCount: List[(T, Long)])(implicit ordering: Ordering[T]): T => Int
}
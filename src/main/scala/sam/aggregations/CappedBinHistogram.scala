package sam.aggregations

import sam.aggregations.TypeAliases.{MergeStrategy, Long2}

import scala.collection.mutable

import CappedBinHistogram._

case class CappedBinHistogram(sizeLimit: Int,
                              mergeStrategy: MergeStrategy = defaultMergeStrategy,
                              private val m: mutable.Map[(Long, Long), Long] = mutable.Map.empty)
  extends Aggregator[Map[(Long, Long), Long], Long, CappedBinHistogram] {

  def size: Int = m.size

  def update(e: Long): Unit =
    m.find {
      case ((lower, upper), count) => lower <= e && e <= upper
    } match {
      case Some((key, count)) =>
        m -= key
        m += key -> (count + 1)
      case None =>
        m += ((e, e) -> 1)
        mergeStrategy(m, sizeLimit)
    }

  def result: Map[(Long, Long), Long] = m.toMap

  def update(a: CappedBinHistogram): Unit = {
    a.result.foreach {
      case (key, count) => m += key -> (count + m.getOrElse(key, 0l))
    }
    mergeStrategy(m, sizeLimit)
  }
}

object CappedBinHistogram {
  val defaultMergeStrategy = mergeSmallestCountSum _

  def mergeConsecutive[Rank: Ordering](m: mutable.Map[Long2, Long],
                                       sizeLimit: Int,
                                       ranker: List[(Long2, Long)] => Rank): mutable.Map[Long2, Long] = {
    if (m.size <= sizeLimit) m
    else {
      // TODO Optimization - We might be able to avoid this nested iteration
      (1 to (m.size - sizeLimit)).foreach { _ =>
        val List((firstRange@(firstStart, _), firstCount), (secondRange@(_, secondEnd), secondCount)) =
          m.toList.sortBy(_._1).sliding(2).minBy(ranker)

        m -= firstRange
        m -= secondRange

        m += (firstStart, secondEnd) -> (firstCount + secondCount)
      }
      m
    }
  }

  def mergeSmallestConsecutiveRange(m: mutable.Map[Long2, Long], sizeLimit: Int): mutable.Map[Long2, Long] =
    mergeConsecutive(m, sizeLimit, ranker = {
      case List((firstKey@(firstStart, _), _), (secondKey@(_, secondEnd), _)) =>
        (secondEnd - firstStart, firstKey, secondKey)
    })

  // Merge buckets that have the least amount of information in
  def mergeSmallestCountSum(m: mutable.Map[Long2, Long], sizeLimit: Int): mutable.Map[Long2, Long] =
    mergeConsecutive(m, sizeLimit, ranker = {
      case List((_, countLeft), (_, countRight)) => countLeft + countRight
    })


  def countMapToDensity(m: List[(Long2, Long)]): List[(Long2, Double)] =
    disjointify(m.flatMap(detachEndpoints)).groupBy(_._1).mapValues(_.map(_._2).sum).toList.sortBy(_._1._1)

  def disjointify(endPointsDetached: List[(Long2, Double)]): List[(Long2, Double)] = {
    val lowerEnds = endPointsDetached.map(_._1._1).distinct
    val upperEnds = endPointsDetached.map(_._1._2).distinct

    endPointsDetached.flatMap {
      case pt@((lower, upper), _) if lower == upper => List(pt)
      case pt@(r@(lower, upper), origDensity) =>
        ((lower, 0) +:
          (lowerEnds.filter(_ > lower).filter(_ <= upper).flatMap(l => List((l - 1, 1), (l, 0))) ++
            upperEnds.filter(_ < upper).filter(_ >= lower).flatMap(u => List((u, 1), (u + 1, 0))))
          .distinct.sorted :+
          (upper, 1))
        .grouped(2).map {
          case List((newLower, _), (newUpper, _)) => splitRange(r, newLower, newUpper, origDensity)
        }
    }
  }

  def splitRange(range: Long2, newLowerPart: Long, newUpperPart: Long, origDensity: Double): (Long2, Double) =
    ((newLowerPart, newUpperPart), divideDensity(range._1, range._2, newLowerPart, newUpperPart, origDensity))

  def divideDensity(lower: Long, upper: Long, newLowerPart: Long, newUpperPart: Long, origDensity: Double): Double = {
    val origRange = upper + 1 - lower
    val newRange = newUpperPart + 1 - newLowerPart
    val proportion = newRange.toDouble / origRange.toDouble
    proportion * origDensity
  }

  def detachEndpoints(pt: (Long2, Long)): List[(Long2, Double)] = pt match {
    case ((lower, upper), n) if lower == upper => List((lower, lower) -> n.toDouble)
    case ((lower, upper), 1l) if lower != upper => throw new IllegalArgumentException("Impossible situation")
    case ((lower, upper), n) if upper == lower + 1 =>
      val extraMassPerPoint = (n - 2).toDouble / (upper + 1 - lower)
      List((lower, lower) -> (1.0 + extraMassPerPoint), (upper, upper) -> (1.0 + extraMassPerPoint))
    case ((lower, upper), n) =>
      val extraMassPerPoint = (n - 2).toDouble / (upper + 1 - lower)
      List(
        (lower, lower) -> (1.0 + extraMassPerPoint),
        (lower + 1, upper - 1) -> (extraMassPerPoint * (upper - 1 - lower)),
        (upper, upper) -> (1.0 + extraMassPerPoint)
      )
  }

  def densityToCumulativeDensity(m: List[(Long2, Double)]): List[(Long2, Double)] =
    m.map(_._1).zip(m.drop(1).scanLeft(m.head._2)((cum, cur) => cum + cur._2))
}

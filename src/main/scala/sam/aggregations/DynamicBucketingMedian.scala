package sam.aggregations

import scala.collection.mutable
import RangeUtils._

object TypeAliases {
  type Long2 = (Long, Long)
  type MergeStrategy = (mutable.Map[Long2, Long], Int) => mutable.Map[Long2, Long]
}

import TypeAliases._

// TODO Chop up into smaller objects
object DynamicBucketingMedian {
  trait CumulatativeCount {
    val lower: Long
    val upper: Long
    val count: Long
    val cumCount: Long
  }
  case class CumDisjoint(lower: Long, upper: Long, count: Long, cumCount: Long) extends CumulatativeCount

  case class CumOverlapping(lower: Long, upper: Long, count: Long, cumCount: Long,
                            overlappingMap: List[(Long2, Long)]) extends CumulatativeCount {
    def distribution: List[(Long2, Double)] = densityToCumulativeDensity(countMapToDensity(overlappingMap))
  }

  def divideDensity(lower: Long, upper: Long, newLowerPart: Long, newUpperPart: Long, origDensity: Double): Double = {
    val origRange = upper + 1 - lower
    val newRange = newUpperPart + 1 - newLowerPart
    val proportion = newRange.toDouble / origRange.toDouble
    proportion * origDensity
  }

  def splitRange(range: Long2, newLowerPart: Long, newUpperPart: Long, origDensity: Double): (Long2, Double) =
    ((newLowerPart, newUpperPart), divideDensity(range._1, range._2, newLowerPart, newUpperPart, origDensity))

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

  def countMapToDensity(m: List[(Long2, Long)]): List[(Long2, Double)] =
    disjointify(m.flatMap(detachEndpoints)).groupBy(_._1).mapValues(_.map(_._2).sum).toList.sortBy(_._1._1)

  def mergeOverlappingInfo(sortedMap: List[(Long2, Long)]): List[(Long2, (Long, Option[List[(Long2, Long)]]))] =
    sortedMap.foldLeft(List.empty[(Long2, (Long, Option[List[(Long2, Long)]]))]) {
      case ((r1@(lowerPrev, upperPrev), (countPrev, None)) :: rest, (r2@(lower, upper), count)) if overlap(r1, r2) =>
        ((math.min(lowerPrev, lower), math.max(upperPrev, upper)),
          (count + countPrev, Some(List(r2 -> count, r1 -> countPrev)))) +: rest

      case ((r1@(lowerPrev, upperPrev), (countPrev, Some(m))) :: rest, (r2@(lower, upper), count)) if overlap(r1, r2) =>
        ((math.min(lowerPrev, lower), math.max(upperPrev, upper)), (count + countPrev, Some((r2 -> count) +: m))) +: rest

      case (cum@((r1, (_, None)) :: _), (r2, count)) if !overlap(r1, r2) =>
        (r2, (count, None)) +: cum

      case (cum, (range, count)) =>
        (range, (count, None)) +: cum
    }
    .map {
      case (r, (count, Some(m))) => (r, (count, Some(m.reverse)))
      case other => other
    }
    .reverse

  def densityToCumulativeDensity(m: List[(Long2, Double)]): List[(Long2, Double)] =
    m.map(_._1).zip(m.drop(1).scanLeft(m.head._2)((cum, cur) => cum + cur._2))

  // TODO Need to chop up this method, it's too long
  def medianFromBuckets(m: Map[Long2, Long]): Double = {
    val overlapsMerged@(_, (headCount, _)) :: _ = mergeOverlappingInfo(m.toList.sortBy(_._1))

    val cumulativeCounts: List[CumulatativeCount] =
      overlapsMerged.zip(overlapsMerged.drop(1).scanLeft(headCount)((cum, cur) => cum + cur._2._1))
      .map {
        case (((lower, upper), (count, None)), cumCount) =>
          CumDisjoint(lower, upper, count, cumCount)
        case (((lower, upper), (count, Some(map))), cumCount) =>
          CumOverlapping(lower, upper, count, cumCount, map)
      }

    cumulativeCounts.last.cumCount match {
      case odd if odd % 2 == 1 =>
        val middleIndex = (odd.toDouble / 2).ceil.toLong

        // Assumes each bucket has the endpoints
        cumulativeCounts.find(_.cumCount >= middleIndex).get match {
          // Exact case
          case CumDisjoint(_, end, _, cum) if cum == middleIndex => end.toDouble
          // Exact case
          case CumDisjoint(start, _, count, cum) if (cum - count + 1) == middleIndex => start.toDouble
          // Assumes symmetrical distribution
          case CumDisjoint(start, end, _, _) => (start + end).toDouble / 2
          case cumOverlapping@CumOverlapping(_, _, count, cumCount, _) =>
            val reverseIndexInOverlap = cumCount - middleIndex
            val innerIndex = count - reverseIndexInOverlap
            cumOverlapping.distribution.find(_._2 >= innerIndex) match {
              case Some(((lower, upper), density)) => (lower + upper).toDouble / 2
                // This is probably caused by Double precision problem, should have a regression test for it
                // might be able to reproduce using property based test
              case None => throw new RuntimeException("Weird cumOveralpping = " + cumOverlapping +
                "\ninnerIndex = " + innerIndex + "\ndistribution = " + cumOverlapping.distribution)
            }
        }

      case even =>
        val middleIndex = even / 2

        (cumulativeCounts.find(_.cumCount >= middleIndex).get,
          cumulativeCounts.find(_.cumCount >= middleIndex + 1).get) match {
          case (bucketLeft@CumDisjoint(start, end, _, _), bucketRight) if bucketLeft == bucketRight =>
            (start + end).toDouble / 2
          case (CumDisjoint(_, endLeft, _, _), CumDisjoint(startRight, _, _, _)) =>
            (endLeft + startRight).toDouble / 2
          case (CumDisjoint(_, endLeft, _, _), CumOverlapping(startRight, _, _, _, _)) =>
            (endLeft + startRight).toDouble / 2
          case (CumOverlapping(_, endLeft, _, _, _), CumDisjoint(startRight, _, _, _)) =>
            (endLeft + startRight).toDouble / 2

          case (CumOverlapping(startLeft, endLeft, _, _, _), CumOverlapping(startRight, _, _, _, _))
            if startLeft != startRight =>
            (endLeft + startRight).toDouble / 2
          case (cumOverlapping@CumOverlapping(_, _, count, cumCount, dist), _) =>
            val reverseIndexInOverlap = cumCount - middleIndex
            val innerIndex = count - reverseIndexInOverlap
            cumOverlapping.distribution.find(_._2 >= innerIndex) match {
              case Some(((lower, upper), density)) => (lower + upper).toDouble / 2
              case None => throw new RuntimeException("Weird cumOveralpping = " + cumOverlapping +
                "\ninnerIndex = " + innerIndex + "\ndistribution = " + cumOverlapping.distribution)
            }
        }
    }
  }
}

import DynamicBucketingMedian._

case class DynamicBucketingMedian(sizeLimit: Int)
  extends Aggregator[Double, Long, DynamicBucketingMedian] {
  private[DynamicBucketingMedian] val hist: CappedBinHistogram = new CappedBinHistogram(sizeLimit)

  def size: Int = hist.size

  def update(e: Long): Unit = hist.update(e)

  def result: Double = {
    val m = hist.result
    if (m.isEmpty) throw new IllegalArgumentException("Cannot call result when no updates called")
    else medianFromBuckets(m)
  }

  def update(a: DynamicBucketingMedian): Unit = hist.update(a.hist)
}

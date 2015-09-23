package sam.aggregations

import scala.collection.mutable
import RangeUtils._

// TODO Chop up into smaller objects
object DynamicBucketingMedian {
  // Core of the algorithm, this has the potential for a lot of variations
  def mergeSmallestConsecutive(m: mutable.Map[(Long, Long), Long], sizeLimit: Int): mutable.Map[(Long, Long), Long] = {
    if (m.size <= sizeLimit) m
    else {
      // TODO We might be able to avoid this N^2 iteration
      (1 to (m.size - sizeLimit)).foreach { _ =>
        val List((firstRange@(firstStart, _), firstCount), (secondRange@(_, secondEnd), secondCount)) =
          m.toList.sortBy(_._1).sliding(2).minBy {
            case List((firstKey@(firstStart, _), _), (secondKey@(_, secondEnd), _)) =>
              (secondEnd - firstStart, firstKey, secondKey)
          }

        m -= firstRange
        m -= secondRange

        m += (firstStart, secondEnd) -> (firstCount + secondCount)
      }
      m
    }
  }

  trait CumulatativeCount {
    val lower: Long
    val upper: Long
    val count: Long
    val cumCount: Long
  }

  case class CumDisjoint(lower: Long, upper: Long, count: Long, cumCount: Long) extends CumulatativeCount
  case class CumOverlapping(lower: Long, upper: Long, count: Long, cumCount: Long,
                            overlappingMap: List[((Long, Long), Long)]) extends CumulatativeCount {
    def distribution: List[(Long2, Double)] = densityToCumulativeDensity(countMapToDensity(overlappingMap))
  }

  def divideDensity(lower: Long, upper: Long, newLowerPart: Long, newUpperPart: Long, origDensity: Double): Double = {
    val origRange = upper + 1 - lower
    val newRange = newUpperPart + 1 - newLowerPart
    val proportion = newRange.toDouble / origRange.toDouble
    proportion * origDensity
  }

  def splitRange(range: Long2, newLowerPart: Long, newUpperPart: Long, origDensity: Double): ((Long, Long), Double) =
    ((newLowerPart, newUpperPart), divideDensity(range._1, range._2, newLowerPart, newUpperPart, origDensity))



  def splitAll(pt: (Long2, Double), endPointsDetached: List[(Long2, Double)]): List[(Long2, Double)] = {
    val (r1@(lower, upper), origDensity) = pt
    require(lower <= upper, "dodgy pt = " + pt)

    val overlapping =
      endPointsDetached.filter(p => overlap(r1, p._1) && p._1 != pt._1)
      .filter {
        case (r2@(otherLower, otherUpper), density) if r1 == r2 =>
          false
        case (r2@(otherLower, otherUpper), density) if otherLower <= lower && otherUpper > upper =>
          false
        case (r2@(otherLower, otherUpper), density) if otherLower < lower && otherUpper == upper =>
          false
        case _ => true
      }

    if (overlapping.nonEmpty)
      overlapping.flatMap {
        case (r2@(otherLower, otherUpper), density) if otherLower == lower =>
          List(splitRange(r1, lower, otherUpper, origDensity), splitRange(r1, otherUpper + 1, upper, origDensity))
        case (r2@(otherLower, otherUpper), density) if otherLower > lower && otherUpper >= upper =>
          List(splitRange(r1, lower, otherLower - 1, origDensity), splitRange(r1, otherLower, upper, origDensity))
        case (r2@(otherLower, otherUpper), density) if otherLower > lower && otherUpper < upper =>
          List(splitRange(r1, lower, otherLower - 1, origDensity), splitRange(r1, otherLower, otherUpper, origDensity),
            splitRange(r1, otherUpper + 1, upper, origDensity))
        case (r2@(otherLower, otherUpper), density) if otherLower < lower && otherUpper < upper =>
          List(splitRange(r1, lower, otherUpper, origDensity), splitRange(r1, otherUpper + 1, upper, origDensity))
      }
        // This distinct prob causing the bug - need better way to optimize (groupBy)
//      .distinct
      .flatMap(splitAll(_, overlapping))
    else
      List(pt)
  }

  def detachEndpoints(pt: (Long2, Long)): List[(Long2, Double)] = pt match {
    case ((lower, upper), n) if lower == upper => List((lower, lower) -> n.toDouble)
//    case ((lower, upper), 2l) if upper == lower + 1 => List((lower, lower) -> 1.0, (upper, upper) -> 1.0)
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

  // Method to turn the count map into a distribution
  def countMapToDensity(m: List[((Long, Long), Long)]): List[((Long, Long), Double)] = {
    val endPointsDetached = m.flatMap(detachEndpoints)

    val disjoint = endPointsDetached.sortBy(_._1._1).flatMap(splitAll(_, endPointsDetached))

    disjoint.groupBy(_._1).mapValues(_.map(_._2).sum).toList.sortBy(_._1._1)
  }

  type Long2 = (Long, Long)

  def mergeOverlappingInfo(sortedMap: List[(Long2, Long)]): List[(Long2, (Long, Option[List[(Long2, Long)]]))] =
    sortedMap.foldLeft(List.empty[((Long, Long), (Long, Option[List[((Long, Long), Long)]]))]) {
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

  def medianFromBuckets(m: Map[(Long, Long), Long]): Double = {
    val overlapsMerged@(_, (headCount, _)) :: _ = mergeOverlappingInfo(m.toList.sortBy(_._1))

//    println("overlapsMerged = " + overlapsMerged)

    val cumulativeCounts: List[CumulatativeCount] =
      overlapsMerged.zip(overlapsMerged.drop(1).scanLeft(headCount)((cum, cur) => cum + cur._2._1))
      .map {
        case (((lower, upper), (count, None)), cumCount) =>
          CumDisjoint(lower, upper, count, cumCount)
        case (((lower, upper), (count, Some(map))), cumCount) =>
//          CumDisjoint(lower, upper, count, cumCount)
                  CumOverlapping(lower, upper, count, cumCount, map)
      }

//    println("cumulativeCounts = " + cumulativeCounts)

    // Produce cumCounts as before

    // bucket, totalUpToAndIncludingBucket
    //    val cumulativeCounts: List[CumulatativeCount] =
    //      sorted.zip(sorted.drop(1).scanLeft(headCount)((cum, cur) => cum + cur._2))
    //      .map {
    //        case (((lower, upper), count), cumCount) => CumDisjoint(lower, upper, count, cumCount)
    //      }

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
//            println("Got here")
            // I.e. how far back we need to go from the right hand side
            val reverseIndexInOverlap = cumCount - middleIndex
            // I.e. how far in we need to go
            val innerIndex = count - reverseIndexInOverlap
            cumOverlapping.distribution.find(_._2 >= innerIndex) match {
              case Some(((lower, upper), density)) => (lower + upper).toDouble / 2
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
          case (CumOverlapping(startLeft, endLeft, _, _, _), CumOverlapping(startRight, _, _, _, _))
          if startLeft != startRight =>
            (endLeft + startRight).toDouble / 2
          case (cumOverlapping@CumOverlapping(_, _, count, cumCount, dist), _) =>
//            println("Got here")
            // I.e. how far back we need to go from the right hand side
            val reverseIndexInOverlap = cumCount - middleIndex
            // I.e. how far in we need to go
            val innerIndex = count - reverseIndexInOverlap
            cumOverlapping.distribution.find(_._2 >= innerIndex) match {
              case Some(((lower, upper), density)) => (lower + upper).toDouble / 2
            }
        }
    }
  }
}

import DynamicBucketingMedian._

// Not thread safe
case class DynamicBucketingMedian(sizeLimit: Int, private val m: mutable.Map[(Long, Long), Long] = mutable.Map.empty)
  extends Median[DynamicBucketingMedian] {

  def size: Int = m.size
  def getMap: Map[(Long, Long), Long] = m.toMap

  def update(e: Long): Unit =
    m.find {
      case ((lower, upper), count) => lower <= e && e <= upper
    } match {
      case Some((key, count)) =>
        m -= key
        m += key -> (count + 1)
      case None =>
        m += ((e, e) -> 1)
        mergeSmallestConsecutive(m, sizeLimit)
    }

  def result: Double =
    if (m.isEmpty) throw new IllegalArgumentException("Cannot call result when no updates called")
    else medianFromBuckets(m.toMap)

  def update(other: DynamicBucketingMedian): Unit = {
    other.getMap.foreach {
      case (key, count) => m += key -> (count + m.getOrElse(key, 0l))
    }
    mergeSmallestConsecutive(m, sizeLimit)
  }
}

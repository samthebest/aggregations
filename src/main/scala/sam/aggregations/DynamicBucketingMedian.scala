package sam.aggregations

import scala.collection.mutable
import RangeUtils._

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
                            distribution: List[((Long, Long), Double)]) extends CumulatativeCount

  def divideDensity(lower: Long, upper: Long, newLowerPart: Long, newUpperPart: Long, origDensity: Double): Double = {
    val origRange = upper + 1 - lower
    val newRange = newUpperPart + 1 - newLowerPart
    val proportion = newRange.toDouble / origRange.toDouble
    proportion * origDensity
  }

  def splitRange(range: Long2, newLowerPart: Long, newUpperPart: Long, origDensity: Double): ((Long, Long), Double) =
    ((newLowerPart, newUpperPart), divideDensity(range._1, range._2, newLowerPart, newUpperPart, origDensity))

  // Method to turn the count map into a distribution
  def countMapToDensity(m: List[((Long, Long), Long)]): List[((Long, Long), Double)] = {
    // Split each into a density range

    val splitted = m.flatMap {
      case ((lower, upper), n) if lower == upper => List((lower, lower) -> n.toDouble)
      case ((lower, upper), 2l) if upper == lower + 1 => List((lower, lower) -> 1.0, (upper, upper) -> 1.0)
      case ((lower, upper), 2l) => List((lower, lower) -> 1.0, (lower + 1, upper - 1) -> 0.0, (upper, upper) -> 1.0)
    }

    println("splitted = " + splitted)

    // This needs to be recursive to catch the cases when we have multiple overlaying ones

    val disjoint = splitted.sortBy(_._1._1).flatMap {
      case pt@(r1@(lower, upper), origDensity) =>
        // Take each other range and split this according to it until we run out
        val overlapping = splitted.takeWhile(p => overlap(r1, p._1))
        if (overlapping.nonEmpty)
          overlapping.flatMap {
            case (r2@(otherLower, otherUpper), density) if r1 == r2 => List(pt)
            case (r2@(otherLower, otherUpper), density) if otherLower <= lower && otherUpper > upper => List(pt)
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
          .distinct
        else
          List(pt)

    }

    println("disjoint = " + disjoint)

    disjoint.groupBy(_._1).mapValues(_.map(_._2).sum).toList.sortBy(_._1._1)

  }


  // Method to approximate the median based on the index within the distribution
  // (easy way to test is using large spikes)

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

  def medianFromBuckets(m: Map[(Long, Long), Long]): Double = {
    val overlapsMerged@(_, (headCount, _)) :: _ = mergeOverlappingInfo(m.toList.sortBy(_._1))

    val cumulativeCounts: List[CumulatativeCount] =
      overlapsMerged.zip(overlapsMerged.drop(1).scanLeft(headCount)((cum, cur) => cum + cur._2._1))
      .map {
        case (((lower, upper), (count, None)), cumCount) =>
          CumDisjoint(lower, upper, count, cumCount)
        case (((lower, upper), (count, Some(map))), cumCount) =>
          CumDisjoint(lower, upper, count, cumCount)
        //          CumOverlapping(lower, upper, count, cumCount, Map.empty)
      }

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
        }

      case even =>
        val middleIndex = even / 2

        (cumulativeCounts.find(_.cumCount >= middleIndex).get,
          cumulativeCounts.find(_.cumCount >= middleIndex + 1).get) match {
          case (bucketLeft@CumDisjoint(start, end, _, _), bucketRight) if bucketLeft == bucketRight =>
            (start + end).toDouble / 2
          case (CumDisjoint(_, endLeft, _, _), CumDisjoint(startRight, _, _, _)) =>
            (endLeft + startRight).toDouble / 2

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

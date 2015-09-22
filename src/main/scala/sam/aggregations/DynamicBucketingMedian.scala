package sam.aggregations

import scala.collection.mutable
import RangeUtils._

object DynamicBucketingMedian {
  // Assumes disjoint
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
                            distribution: Map[(Long, Long), Double]) extends CumulatativeCount

  def mergeOverlappingInfo(sortedMap: List[((Long, Long), Long)]): List[((Long, Long), (Long, Option[Map[(Long, Long), Long]]))] = {
    val empty = List.empty[((Long, Long), (Long, Option[Map[(Long, Long), Long]]))]
    sortedMap.foldLeft(empty) {
      case ((r1@(lowerPrev, upperPrev), (countPrev, None)) :: rest, (r2@(lower, upper), count))
        if rangesOverlap(r1, r2) =>

        val mergedRange = (math.min(lowerPrev, lower), math.max(upperPrev, upper))
        val map = Map(r1 -> countPrev, r2 -> count)
        (mergedRange, (count + countPrev, Some(map))) +: rest

      case ((r1@(lowerPrev, upperPrev), (countPrev, Some(m))) :: rest, (r2@(lower, upper), count))
        if rangesOverlap(r1, r2) =>

        val mergedRange = (math.min(lowerPrev, lower), math.max(upperPrev, upper))
        val map = m + (r2 -> count)
        (mergedRange, (count + countPrev, Some(map))) +: rest

      case (cum@((r1@(lowerPrev, upperPrev), (countPrev, None)) :: rest), (r2@(lower, upper), count))
        if !rangesOverlap(r1, r2) =>

        (r2, (count, None)) +: cum

      case (cum, (range, count)) =>
        (range, (count, None)) +: cum
    }
    .reverse
  }

  def medianFromDisjointBuckets(m: Map[(Long, Long), Long]): Double = {
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
    else medianFromDisjointBuckets(m.toMap)

  def update(other: DynamicBucketingMedian): Unit = {
    other.getMap.foreach {
      case (key, count) => m += key -> (count + m.getOrElse(key, 0l))
    }
    mergeSmallestConsecutive(m, sizeLimit)
  }
}

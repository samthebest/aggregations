package sam.aggregations

import scala.collection.mutable
import Utils._

object TypeAliases {
  type Long2 = (Long, Long)
  type MergeStrategy = (mutable.Map[Long2, Long], Int) => mutable.Map[Long2, Long]
}

import TypeAliases._, CappedBinHistogram._

object MedianEstimator {
  trait CumulatativeCount {
    val lower: Long
    val upper: Long
    val count: Long
    val cumCount: Long
  }

  case class CumDisjoint(lower: Long, upper: Long, count: Long, cumCount: Long) extends CumulatativeCount

  case class CumOverlapping(lower: Long, upper: Long, count: Long, cumCount: Long,
                            overlappingMap: List[(Long2, Long)]) extends CumulatativeCount {
    def distribution: List[(Long2, Double)] =
      densityToCumulativeDensity(countMapToDensity(overlappingMap))
  }

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

  val floatingPointHackAmount = 0.000001

  def medianFromOverlap(cumOverlapping: CumOverlapping, middleIndex: Long): Double = {
    val innerIndex = cumOverlapping.count - (cumOverlapping.cumCount - middleIndex)
    val Some(((lower, upper), _)) = cumOverlapping.distribution.find(_._2 + floatingPointHackAmount >= innerIndex)
    (lower + upper).toDouble / 2
  }

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
          case cumOverlapping: CumOverlapping => medianFromOverlap(cumOverlapping, middleIndex)
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
          case (cumOverlapping: CumOverlapping, _) => medianFromOverlap(cumOverlapping, middleIndex)
        }
    }
  }
}

import MedianEstimator._

case class MedianEstimator(sizeLimit: Int)
  extends Aggregator[Double, Long, MedianEstimator] {
  private[MedianEstimator] val hist: CappedBinHistogram = new CappedBinHistogram(sizeLimit)

  def size: Int = hist.size

  def update(e: Long): Unit = hist.update(e)

  def result: Double = {
    val m = hist.result
    if (m.isEmpty) throw new IllegalArgumentException("Cannot call result when no updates called")
    else medianFromBuckets(m)
  }

  def update(a: MedianEstimator): Unit = hist.update(a.hist)
}

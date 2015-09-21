package sam.aggregations

import scala.collection.mutable

case class MiddleRange(start: Long = 0l,
                       end: Long = 0l,
                       totalCountLeft: Long = 0l,
                       middleCount: Option[Long] = Some(1l),
                       totalCountRight: Long = 0l,
                       exactMedian: Option[Double] = None)

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

  // When they overlap, we move the next bucket at the previous one + 1??

  // Actually no, all we can do, when we apply same logic is eliminate the start point - nothing more.

  // A simple MVP would be to rebalance.

  // The really hard case for overlapping buckets is when one bucket completely covers another.  In this situation we
  // still have information - particularly that at least one element exists at the end points, we also have some
  // "distribution" information - or density information. This can give us more information (think about the
  // simplest non-trivial case, each bucket has 3 elements.

  // We could handle this case seperately, by merging all these together, but keeping a record of there originals
  // Then when the middle index lies inside one of the big guys that covers the others, we can have some other method
  // that handles it.  We essentially have a well defined density function, where we can assume it's uniform.

  // Things get uber hard when we have constructed the map from many many other maps, this could mean we have a
  // horrible lattice structure.

  // Yes plan should be to merge overlapping buckets but keep their density information (actually we can compute
  // exact density information e.g. (1, 3) -> 3 goes to (1 -> 1.5, 2 -> 1.0, 3 -> 1.5)

  def medianFromDisjointBuckets(m: Map[(Long, Long), Long]): Double = {
    val sorted@(_, headCount) :: _ = m.toList.sortBy(_._1)

    // bucket, totalUpToAndIncludingBucket
    val cumulativeCounts: List[(((Long, Long), Long), Long)] =
      sorted.zip(sorted.drop(1).scanLeft(headCount)((cum, cur) => cum + cur._2))

    cumulativeCounts.last._2 match {
      case odd if odd % 2 == 1 =>
        val middleIndex = (odd.toDouble / 2).ceil.toLong

        // Assumes each bucket has the endpoints
        cumulativeCounts.find(_._2 >= middleIndex).get match {
          // Exact case
          case (((_, end), _), cum) if cum == middleIndex => end.toDouble
          // Exact case
          case (((start, _), count), cum) if (cum - count + 1) == middleIndex => start.toDouble
          // Assumes symmetrical distribution
          case (((start, end), _), _) => (start + end).toDouble / 2
        }

      case even =>
        val middleIndex = even / 2

        (cumulativeCounts.find(_._2 >= middleIndex).get, cumulativeCounts.find(_._2 >= middleIndex + 1).get) match {
          case (bucketLeft@(((start, end), _), _), bucketRight) if bucketLeft == bucketRight =>
            (start + end).toDouble / 2
          case ((((_, endLeft), _), _), (((startRight, _), _), _)) =>
            (endLeft + startRight).toDouble / 2

        }
    }
  }
}

import DynamicBucketingMedian._

// Not thread safe
class DynamicBucketingMedian(val sizeLimit: Int) extends Median[DynamicBucketingMedian] {
  def size: Int = m.size

  def getMap: Map[(Long, Long), Long] = m.toMap

  private val m: mutable.Map[(Long, Long), Long] = mutable.Map.empty

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

  // A very simple implementation would be to sample from the right hand side semi-uniformly
  def update(m: DynamicBucketingMedian): Unit = ???
}

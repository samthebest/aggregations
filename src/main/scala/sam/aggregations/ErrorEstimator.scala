package sam.aggregations

import scala.util.Random

object ErrorEstimator {
  val rand = new Random()
  def uniformDistribution(median: Median, n: Int, max: Int): Double =
    relativeError((1 to n).map(_ => rand.nextInt(max).toLong), median)

  def correctMedian(numbers: IndexedSeq[Long]): Double = {
    val exactMedian = new ExactMedian()
    numbers.foreach(exactMedian.update)
    exactMedian.result
  }

  def relativeError(numbers: IndexedSeq[Long], median: Median): Double = {
    val correct = correctMedian(numbers)
    numbers.foreach(median.update)
    val medianEstimate = median.result
    val absaluteError = math.abs(correct - medianEstimate)
    absaluteError / correct
  }
}

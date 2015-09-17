package sam.aggregations

import breeze.stats.distributions.Gaussian

import scala.util.Random

object ErrorEstimator {
  val rand = new Random()

  def normalishSample(n: Int, max: Int): IndexedSeq[Long] = {
    val dist = new Gaussian(max / 2.0, max / 6.0)
    (1 to n).map(_ => math.floor(dist.draw())).map {
      case i if i < 0 => 0L
      case i if i > max => max.toLong
      case i => i.toLong
    }
  }

  def normalDistribution(median:  Median, n: Int, max: Int): Double = relativeError(normalishSample(n, max), median)

  def uniformDistribution(median: Median, n: Int, max: Int): Double =
    relativeError((1 to n).map(_ => rand.nextInt(max).toLong), median)

  def correctMedian(numbers: Seq[Long]): Double = {
    val exactMedian = new ExactMedian()
    numbers.foreach(exactMedian.update)
    exactMedian.result
  }

  def relativeError(numbers: Seq[Long], median: Median): Double = {
    val correct = correctMedian(numbers)
    numbers.foreach(median.update)
    val medianEstimate = median.result
    val absaluteError = math.abs(correct - medianEstimate)
    absaluteError / correct
  }
}

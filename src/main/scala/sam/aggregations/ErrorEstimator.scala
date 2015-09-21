package sam.aggregations

import breeze.stats.distributions.Gaussian
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.util.Random

case class Report(errorsAndNumExamples: List[(Double, Int)] = Nil,
                  averageError: Double = 1.0,
                  bestError: Double = 1.0,
                  worstError: Double = 1.0,
                  averageNumExamples: Int = 0,
                  mostNumExamples: Int = 0,
                  leastNumExamples: Int = 0) {
  def pretty: String =
    "Average Error: " + averageError + "\n" +
      "Best Error: " + bestError + "\n" +
      "Worst Error: " + worstError + "\n" +
      "Average num examples: " + averageNumExamples + "\n" +
      "Most num examples: " + mostNumExamples + "\n" +
      "Least num examples: " + leastNumExamples + "\n" +
      "errorsAndNumExamples:\n" + errorsAndNumExamples.map(p => p._1.toString + "\t" + p._2).mkString("\n")
}

object ErrorEstimator {
  // TODO Wrong - we ought to be using combineByKey, not groupBy and mapValues
  def fromTestData[T: ClassTag](testData: RDD[(T, Long)],
                                medianFac: Int => Median[_] = _ => new ExactMedian(),
                                memoryCap: Int = 1000): Report = {
    val errorsAndNumExamples =
      testData.groupBy(_._1).mapValues(_.map(_._2).toArray).flatMap {
        case (t, data) if data.length <= memoryCap => None
        case (t, data) => Some((relativeError(data, medianFac(memoryCap)), data.length))
      }
      .collect().toList

    val (errors, numExamples) = errorsAndNumExamples.unzip

    val size = errorsAndNumExamples.size

    if (size == 0) Report()
    else Report(
      errorsAndNumExamples = errorsAndNumExamples,
      averageError = errors.sum / size,
      bestError = errors.min,
      worstError = errors.max,
      averageNumExamples = numExamples.sum / size,
      mostNumExamples = numExamples.max,
      leastNumExamples = numExamples.min
    )
  }

  val rand = new Random()

  def normalishSample(n: Int, max: Int): IndexedSeq[Long] = {
    val dist = new Gaussian(max / 2.0, max / 6.0)
    (1 to n).map(_ => math.floor(dist.draw())).map {
      case i if i < 0 => 0L
      case i if i > max => max.toLong
      case i => i.toLong
    }
  }

  def normalDistribution(median: Median[_], n: Int, max: Int): Double = relativeError(normalishSample(n, max), median)

  def uniformDistribution(median: Median[_], n: Int, max: Int): Double =
    relativeError((1 to n).map(_ => rand.nextInt(max).toLong), median)

  def correctMedian(numbers: Seq[Long]): Double = {
    val exactMedian = new ExactMedian()
    numbers.foreach(exactMedian.update)
    exactMedian.result
  }

  def relativeError(numbers: Seq[Long], median: Median[_]): Double = {
    val correct = correctMedian(numbers)
    numbers.foreach(median.update)
    val medianEstimate = median.result
    val absaluteError = math.abs(correct - medianEstimate)
    absaluteError / correct
  }
}

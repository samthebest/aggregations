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
  def fromTestData[T: ClassTag, M <: Median[M]](testData: RDD[(T, Long)],
                                                medianFac: Int => M,
                                                memoryCap: Int = 1000): Report = {
    val estimates: RDD[(T, Double)] =
      testData.combineByKey(
        createCombiner = (l: Long) => {
          val m = medianFac(memoryCap)
          m.update(l)
          m
        },
        mergeValue = (m: M, l: Long) => {
          m.update(l)
          m
        },
        mergeCombiners = (m1: M, m2: M) => {
          m1.update(m2)
          m1
        },
        numPartitions = 500
      )
      .mapValues(_.result)

    val correctMedianAndCounts: RDD[(T, (Double, Int))] =
      testData.groupBy(_._1).mapValues(_.map(_._2).toArray).flatMap {
        case (t, data) if data.length <= memoryCap => None
        case (t, data) => Some((t, (correctMedian(data), data.length)))
      }

    val errorsAndNumExamples: List[(Double, Int)] =
      estimates.join(correctMedianAndCounts).map {
        case (_, (estimate, (correct, count))) => (relativeError(estimate, correct), count)
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
    numbers.foreach(median.update)
    relativeError(median.result, correctMedian(numbers))
  }

  def relativeError(estimate: Double, correct: Double): Double = math.abs(correct - estimate) / correct
}

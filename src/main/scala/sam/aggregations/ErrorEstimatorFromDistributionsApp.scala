package sam.aggregations

import breeze.stats.distributions.{Gaussian, Rand}
import org.rogach.scallop.ScallopConf

// Only runs for the sliding window mode.
object ErrorEstimatorFromDistributionsApp {
  def main(args: Array[String]): Unit = {
    val conf = new ScallopConf(args) {
      val runs = opt[Int](default = Some(10), descr = "Number of runs", validate = _ > 0)
    }

    SlidingWindowErrorEstimator.testCases(
      cases =
        (for {
          memoryLimit <- List(20, 50, 100, 500)
          totalDataPoints <- List(10, 20, 50, 100, 200, 300, 1000, 2000, 3000, 4000, 10000) if totalDataPoints > memoryLimit
        } yield TestCase(totalDataPoints, memoryLimit))
        .toIterator,
      runs = conf.runs()
    )
    .map(_.toTSV).foreach(println)
  }
}

case class TestCase(totalDataPoints: Int, memoryLimit: Int)

case class Result(totalDataPoints: Int,
                  memoryLimit: Int,
                  averageError: Double,
                  bestError: Double,
                  worstError: Double,
                  averageDistinctCounts: Double,
                  errorsAndDistinctCounts: List[(Double, Int)]) {
  def toTSV: String = productIterator.toList.dropRight(1).mkString("\t") + "\t" +
    errorsAndDistinctCounts.map(_.productIterator.mkString(",")).mkString("\t")
}

object Result {
  def apply(totalDataPoints: Int,
            memoryLimit: Int,
            errorsAndDistinctCounts: List[(Double, Int)]): Result = Result(
    totalDataPoints = totalDataPoints,
    memoryLimit = memoryLimit,
    averageError = errorsAndDistinctCounts.map(_._1).sum / errorsAndDistinctCounts.map(_._1).size,
    bestError = errorsAndDistinctCounts.map(_._1).min,
    worstError = errorsAndDistinctCounts.map(_._1).max,
    averageDistinctCounts = errorsAndDistinctCounts.map(_._2).sum.toDouble / errorsAndDistinctCounts.map(_._2).size,
    errorsAndDistinctCounts = errorsAndDistinctCounts
  )
}

object SlidingWindowErrorEstimator {
  val medianFac = MedianEstimator.apply _

  def cappedNormal(cap: Int): Rand[Long] = new Gaussian(cap / 2.0, cap / 6.0).map(math.floor).map {
    case i if i < 0 => 0L
    case i if i > cap => cap.toLong
    case i => i.toLong
  }

  def testCases(cases: Iterator[TestCase], runs: Int = 100, rand: Rand[Long] = cappedNormal(10000)): Iterator[Result] =
    cases.map {
      case TestCase(totalDataPoints, memoryLimit) =>
        Result(
          totalDataPoints = totalDataPoints,
          memoryLimit = memoryLimit,
          errorsAndDistinctCounts =
            (1 to runs)
            .map(_ => rand.sample(totalDataPoints).toList)
            .map(data => (ErrorEstimator.relativeError(data, medianFac(memoryLimit)), data.distinct.size)).toList
        )
    }
}


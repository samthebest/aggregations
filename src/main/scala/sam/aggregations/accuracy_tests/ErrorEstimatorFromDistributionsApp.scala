package sam.aggregations.accuracy_tests

import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

sealed trait ExperimentMode
case object MapReduce extends ExperimentMode
case object SlidingWindow extends ExperimentMode

// Only runs for the sliding window mode.
object ErrorEstimatorFromDistributionsApp {
  val modes = Set(MapReduce, SlidingWindow)

  def main(args: Array[String]): Unit = {
    val conf = new ScallopConf(args) {
      val runs = opt[Int](default = Some(10), descr = "Number of runs", validate = _ > 0)
      val mode = opt[String](default = Some(MapReduce.toString),
        descr = s"Choose either ${SlidingWindow.toString} or ${MapReduce.toString}",
        validate = modes.map(_.toString))
    }

    val cases =
      (for {
        memoryLimit <- List(20, 50, 100, 500)
        totalDataPoints <- List(10, 20, 50, 100, 200, 300, 1000, 2000, 3000, 4000, 10000) if totalDataPoints > memoryLimit
      } yield TestCase(totalDataPoints, memoryLimit))
      .toIterator

    (modes.find(_.toString == conf.mode()): @unchecked) match {
      case Some(MapReduce) =>
        ErrorEstimator.runExperimentsMapReduce(
          cases = cases,
          sc = new SparkContext(new SparkConf().setAppName("Test Median").setMaster("local")),
          runs = conf.runs()
        )
        .map(_.toTSV).foreach(println)
      case Some(SlidingWindow) =>
        ErrorEstimator.runExperimentsSlidingWindow(
          cases = cases,
          runs = conf.runs()
        )
        .map(_.toTSV).foreach(println)
    }
  }
}

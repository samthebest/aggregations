package sam.aggregations

import org.apache.spark.{SparkContext, SparkConf}

// TODO A similar one for Normal and Uniform
object ErrorEstimatorFromDataApp {
  def main(args: Array[String]): Unit = {
    // Use Scallop when this gets more complicated
    val testDataPath = args.headOption.getOrElse("/user/savagesa/median-test-data")

    val confMap =
      Map(
        "spark.default.parallelism" -> 100.toString,
        "spark.hadoop.validateOutputSpecs" -> "false",
        "spark.storage.memoryFraction" -> 0.6.toString,
        "spark.shuffle.memoryFraction" -> 0.3.toString,
        "spark.akka.frameSize" -> 500.toString,
        "spark.akka.askTimeout" -> 100.toString,
        "spark.worker.timeout" -> 150.toString,
        "spark.shuffle.consolidateFiles" -> "true",
        "spark.core.connection.ack.wait.timeout" -> "600"
      )

    @transient val conf: SparkConf =
      new SparkConf().setAppName("Test Median")
      .setAll(confMap)

    @transient val sc = new SparkContext(conf)

    val report =
      ErrorEstimator.fromTestData(
        testData = sc.textFile(testDataPath).map(_.split("\t").toList).map {
          case key :: value :: Nil => (key, value.toLong)
        },
        medianFac = (i: Int) => DynamicBucketingMedian(i),
        memoryCap = 100
      )

    println("report:\n" + report.pretty)

    if (acceptanceTest(report)) {
      println("Test passed")
      System.exit(0)
    } else {
      println("Test FAILED FOOL")
      System.exit(1)
    }
  }

  def acceptanceTest(report: Report): Boolean = report.worstError < 0.03 && report.averageError < 0.02
}

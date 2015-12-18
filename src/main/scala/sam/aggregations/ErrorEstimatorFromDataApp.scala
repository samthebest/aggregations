package sam.aggregations

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection
import scala.util.Random

// spark-submit --master yarn-client --driver-memory 10g --executor-memory 20g --executor-cores 10 --num-executors 5 \
// --class sam.aggregations.ErrorEstimatorFromDataApp aggregations-assembly-0.1.0.jar /user/savagesa/median-test-data 100 2>&1 | tee aggregations.log
object ErrorEstimatorFromDataApp {
  def main(args: Array[String]): Unit = {
    // Use Scallop when args gets more complicated

    val testDataPath = args(0)
    val memory = args(1).toInt
    val divisor = args(2).toInt

    println("divisor = " + divisor)
    println("memory = " + memory)
    println("testDataPath = " + testDataPath)

    def acceptanceTest(report: FromTestDataReport): Boolean = report.worstError <= 2.0 / memory && report.averageError <= 1.0 / memory

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
        testData =
          sc.textFile(testDataPath).map(_.split("\t").toList).map {
            case key :: value :: Nil => (new Random().nextInt(15000), (key, value.toLong / divisor))
          }
          .groupByKey().flatMap(_._2),
        median = new MedianEstimator(memory),
        memoryCap = memory
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
}

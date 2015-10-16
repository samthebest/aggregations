package sam.aggregations

import org.apache.spark.{SparkConf, SparkContext}
import org.specs2.mutable.Specification

object StaticSparkContext {
  val staticSc = new SparkContext(
    new SparkConf().setMaster("local").setAppName("Tests")
  )
}

import StaticSparkContext._, MedianSpecUtils._

class ErrorEstimatorSpecs extends Specification with Serializable {
  sequential

  case class MockMedian(fixedResult: Double) extends Aggregator[Double, Long, MockMedian] {
    def result: Double = fixedResult
    def update(e: Long): Unit = ()
    def update(m: MockMedian): Unit = ()
  }

  "ErrorEstimator" should {
    "Correctly estimate 10% error for mock median" in {
      val numbers = Seq(1, 3, 50, 50, 60, 88).map(_.toLong)
      ErrorEstimator.relativeError(numbers, MockMedian(55.0)) must_=== 0.1
    }

    "Correctly estimate 5% error for mock median" in {
      val numbers = Seq(1, 3, 50, 50, 60, 88).map(_.toLong)
      ErrorEstimator.relativeError(numbers, MockMedian(52.5)) must_=== 0.05
    }

    "Correctly estimate 0% error for mock median" in {
      val numbers = Seq(1, 3, 50, 50, 60, 88).map(_.toLong)
      ErrorEstimator.relativeError(numbers, MockMedian(50.0)) must_=== 0.0
    }

    "Correctly estimate zero error for sampling from a uniform distribution for the ExactMedian" in {
      uniformDistribution(new ExactMedian(), 100, 10) must_=== 0.0
    }

    "Correctly estimate zero error for sampling from a normal distribution for the ExactMedian" in {
      normalDistribution(new ExactMedian(), 100, 10) must_=== 0.0
    }

    "Produce empty report for empty RDD" in {
      ErrorEstimator.fromTestData(staticSc.makeRDD(Nil: List[(String, Long)]),
        i => new ExactMedian()) must_=== FromTestDataReport(Nil, 1.0, 1.0, 1.0)
    }

    "Produce single report for single common key RDD" in {
      val singleKeyRDD = staticSc.makeRDD(Seq(1, 3, 50, 50, 60, 88).map(i => ("common-key", i.toLong)))
      ErrorEstimator.fromTestData(singleKeyRDD, i => MockMedian(55.0), memoryCap = 3) must_===
        FromTestDataReport(List((0.1, 6)), 0.1, 0.1, 0.1, 6, 6, 6)
    }

    "Produce single report for single common key RDD and excludes silly example" in {
      val singleKeyRDD = staticSc.makeRDD(
        Seq(1, 3, 50, 50, 60, 88).map(i => ("common-key-1", i.toLong)) ++
          Seq(1, 3, 50).map(i => ("common-key-2", i.toLong))
      )
      ErrorEstimator.fromTestData(singleKeyRDD, i => MockMedian(55.0), memoryCap = 3) must_===
        FromTestDataReport(List((0.1, 6)), 0.1, 0.1, 0.1, 6, 6, 6)
    }

    "Produce two report for two non-trivial common key RDD and excludes silly example" in {
      val singleKeyRDD = staticSc.makeRDD(
        Seq(1, 3, 50, 50, 60, 88).map(i => ("common-key-1", i.toLong)) ++
          Seq(1, 3, 5, 100, 100, 609, 898, 999).map(i => ("common-key-2", i.toLong)) ++
          Seq(1, 3, 50).map(i => ("common-key-3", i.toLong))
      )
      ErrorEstimator.fromTestData(singleKeyRDD, i => MockMedian(55.0), memoryCap = 3) must_===
        FromTestDataReport(
          errorsAndNumExamples = List((0.1, 6), (0.45, 8)),
          averageError = 0.275,
          bestError = 0.1,
          worstError = 0.45,
          averageNumExamples = 7,
          mostNumExamples = 8,
          leastNumExamples = 6
        )
    }
  }
}

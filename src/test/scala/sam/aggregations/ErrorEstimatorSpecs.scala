package sam.aggregations

import org.specs2.mutable.Specification

class ErrorEstimatorSpecs extends Specification {
  def mockMedian(fixedResult: Double): Median = new Median {
    def result: Double = fixedResult
    def update(e: Long): Unit = ()
    def update(m: Median): Unit = ()
  }

  "ErrorEstimator" should {
    "Correctly estimate 10% error for mock median" in {
      val numbers = Seq(1, 3, 50, 50, 60, 88).map(_.toLong)
      ErrorEstimator.relativeError(numbers, mockMedian(55.0)) must_=== 0.1
    }

    "Correctly estimate 5% error for mock median" in {
      val numbers = Seq(1, 3, 50, 50, 60, 88).map(_.toLong)
      ErrorEstimator.relativeError(numbers, mockMedian(52.5)) must_=== 0.05
    }

    "Correctly estimate 0% error for mock median" in {
      val numbers = Seq(1, 3, 50, 50, 60, 88).map(_.toLong)
      ErrorEstimator.relativeError(numbers, mockMedian(50.0)) must_=== 0.0
    }

    "Correctly estimate zero error for sampling from a uniform distribution for the ExactMedian" in {
      ErrorEstimator.uniformDistribution(new ExactMedian(), 100, 10) must_=== 0.0
    }

    "Correctly estimate zero error for sampling from a normal distribution for the ExactMedian" in {
      ErrorEstimator.normalDistribution(new ExactMedian(), 100, 10) must_=== 0.0
    }
  }
}

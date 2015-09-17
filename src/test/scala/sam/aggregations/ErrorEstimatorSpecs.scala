package sam.aggregations

import org.specs2.mutable.Specification

class ErrorEstimatorSpecs extends Specification {
  "ErrorEstimator" should {
    "Correctly estimate zero error for sampling from a uniform distribution for the ExactMedian" in {
      ErrorEstimator.uniformDistribution(new ExactMedian(), 1000, 100) must_=== 0.0
    }
  }
}

package sam.aggregations

class ExactMedianSpec extends MedianSpecUtils {
  sequential

  basicMedianSpecs(() => new ExactMedian())
  medianProperties((_: Int) => new ExactMedian())
}

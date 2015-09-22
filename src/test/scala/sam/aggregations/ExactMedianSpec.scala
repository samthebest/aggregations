package sam.aggregations

class ExactMedianSpec extends MedianSpecUtils {
  sequential

  basicMedianSpecs(() => new ExactMedian())
  medianIsCommutative((_: Int) => new ExactMedian())
}

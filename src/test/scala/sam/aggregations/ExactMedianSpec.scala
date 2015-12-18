package sam.aggregations

import scala.collection.mutable

class ExactMedianSpec extends MedianSpecUtils {
  sequential

  basicMedianSpecs[mutable.MutableList[Long], ExactMedian.type](() => ExactMedian)
  medianIsCommutative[mutable.MutableList[Long], ExactMedian.type]((_: Int) => ExactMedian)
}
